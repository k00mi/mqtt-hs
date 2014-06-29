{-# Language PatternSynonyms,
             OverloadedStrings,
             FlexibleContexts,
             DeriveDataTypeable #-}
{-|
Module: MQTT
Copyright: Lukas Braun 2014
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

A MQTT client library.

A simple example, assuming a broker is running on localhost
(needs -XOverloadedStrings):

>>> Just mqtt <- connect def
>>> let f t payload = putStrLn $ "A message was published to " ++ show t ++ ": " ++ show pyload
>>> subscribe mqtt NoConfirm "#" f
>>> publish mqtt Handshake False "some random/topic" "Some content!"
A message was published to "some random/topic": "Some content!"
-}
module MQTT
  ( connect
  , MQTT
  , MQTTConfig(..)
  , def
  , Will(..)
  , subscribe
  , publish
  , disconnect
  , reconnect
  , QoS(..)
  , Topic
  , MsgType(..)
  , send
  , addHandler
  , removeHandler
  , awaitMsg
  ) where

import Control.Applicative
import Control.Concurrent
import Control.Exception hiding (handle)
import Control.Monad
import Data.Attoparsec (parseOnly)
import Data.Bits ((.&.))
import Data.ByteString (hGet, ByteString)
import qualified Data.ByteString as BS
import Data.Foldable (for_)
import qualified Data.Map as M
import Data.Maybe (isJust)
import Data.Word
import Data.Text (Text)
import Data.Typeable (Typeable)
import Data.Unique
import Network
import System.IO (Handle, hClose, hIsEOF)
import System.Timeout (timeout)

import MQTT.Types
import MQTT.Parser
import MQTT.Encoding

-----------------------------------------
-- Interface
-----------------------------------------

-- | Abstract type representing a connection to a broker.
data MQTT
    = MQTT
        { config :: MQTTConfig
        , handle :: MVar Handle
        , handlers :: MVar (M.Map MsgType [(Unique, Message -> IO ())])
        , topicHandlers :: MVar [(Topic, Topic -> ByteString -> IO ())]
        , recvThread :: MVar ThreadId
        }


-- | The various options when establishing a connection.
data MQTTConfig
    = MQTTConfig
        { cHost :: HostName
        , cPort :: PortNumber
        , cClean :: Bool
        , cWill :: Maybe Will
        , cUsername :: Maybe Text
        , cPassword :: Maybe Text
        , cKeepAlive :: Maybe Int
        , cClientID :: Text
        , cConnectTimeout :: Maybe Int
        , cReconnPeriod :: Maybe Int
        }
    deriving (Eq, Show)

-- | Defaults for 'MQTTConfig', connects to a server running on
-- localhost.
def :: MQTTConfig
def = MQTTConfig
        "localhost" 1883 True Nothing Nothing Nothing Nothing
        "mqtt-haskell" Nothing Nothing

-- | A Will message is published by the broker if a client disconnects
-- without sending a DISCONNECT.
data Will
    = Will
        { wQoS :: QoS
        , wRetain :: Bool
        , wTopic :: Topic
        , wMsg :: Text
        }
    deriving (Eq, Show)


-- | Establish a connection.
connect :: MQTTConfig -> IO (Maybe MQTT)
connect conf = do
    h <- connectTo (cHost conf) (PortNumber $ cPort conf)
    mqtt <- MQTT conf
              <$> newMVar h
              <*> newMVar M.empty
              <*> newMVar []
              <*> newEmptyMVar
    mCode <- handshake mqtt
    if mCode == Just 0
      then Just mqtt <$ do forkIO (recvLoop mqtt) >>= putMVar (recvThread mqtt)
                           addHandler mqtt PUBLISH (publishHandler mqtt)
      else Nothing <$ hClose h

-- | Send a 'Message' to the server.
send :: MQTT -> Message -> IO ()
send mqtt msg = do
    h <- readMVar (handle mqtt)
    writeTo h msg

-- | Register a callback that gets invoked whenever a 'Message' of the
-- given 'MsgType' is received. Returns the ID of the handler which can be
-- passed to 'removeHandler'.
addHandler :: MQTT -> MsgType -> (Message -> IO ()) -> IO Unique
addHandler mqtt msgType handler = do
    id <- newUnique
    modifyMVar_ (handlers mqtt) $ \hs ->
      return $ M.insertWith' (++) msgType [(id, handler)] hs
    return id

-- | Remove the handler with the given ID.
removeHandler :: MQTT -> MsgType -> Unique -> IO ()
removeHandler mqtt msgType id = modifyMVar_ (handlers mqtt) $ \hs ->
    return $ M.adjust (filter ((/= id) . fst)) msgType hs

-- | Subscribe to a 'Topic' with the given 'QoS' and invoke the callback
-- whenever something is published to the 'Topic'. Returns the 'QoS' that
-- was granted by the broker (lower or equal to the one requested).
--
-- The 'Topic' may contain
-- <http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#appendix-a wildcars>.
-- The 'Topic' passed to the callback is the fully expanded version where
-- the message was actually published.
subscribe :: MQTT -> QoS -> Topic -> (Topic -> ByteString -> IO ())
          -> IO (Maybe QoS)
subscribe mqtt qos topic handler = do
    modifyMVar_ (topicHandlers mqtt) $ \hs ->
      return $ (topic, handler) : hs
    msgID <- fromIntegral . hashUnique <$> newUnique
    send mqtt $ Message
                  (Header SUBSCRIBE False Confirm False)
                  (Just (VHOther msgID))
                  (Just (PLSubscribe [(topic, qos)]))
    msg <- awaitMsg mqtt SUBACK (Just msgID)
    return $ do
      PLSubAck [qosGranted] <- payload msg
      return qosGranted

-- | Publish a message to the given 'Topic' at the requested 'QoS' level.
-- The payload can be any sequence of bytes, including none at all. The 'Bool'
-- parameter decides if the server should retain the message for future
-- subscribers to the topic.
--
-- The 'Topic' must not contain
-- <http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#appendix-a wildcards>.
publish :: MQTT -> QoS -> Bool -> Topic -> ByteString -> IO ()
publish mqtt qos retain topic body = do
    msgID <- fromIntegral . hashUnique <$> newUnique
    send mqtt $ Message
                  (Header PUBLISH False qos retain)
                  (Just (VHPublish (PublishHeader topic (Just msgID))))
                  (Just (PLPublish body))
    case qos of
      NoConfirm -> return ()
      Confirm   -> void $ awaitMsg mqtt PUBACK (Just msgID)
      Handshake -> do
        void $ awaitMsg mqtt PUBREC (Just msgID)
        send mqtt $ Message
                      (Header PUBREL False Confirm False)
                      (Just (VHOther msgID))
                      Nothing
        void $ awaitMsg mqtt PUBCOMP (Just msgID)

-- | Block until a 'Message' of the given type, optionally with the given
-- 'MsgID', arrives.
awaitMsg :: MQTT -> MsgType -> Maybe MsgID -> IO Message
awaitMsg mqtt msgType mMsgID = do
    var <- newEmptyMVar
    handlerID <- addHandler mqtt msgType (putMVar var)
    let wait = do
          msg <- readMVar var
          if isJust mMsgID
            then if mMsgID == (varHeader msg >>= getMsgID)
                   then removeHandler mqtt msgType handlerID >> return msg
                   else wait
            else removeHandler mqtt msgType handlerID >> return msg
    wait

-- | Close the connection to the server.
disconnect :: MQTT -> IO ()
disconnect mqtt = do
    h <- takeMVar $ handle mqtt
    writeTo h $
      Message
        (Header DISCONNECT False NoConfirm False)
        Nothing
        Nothing
    readMVar (recvThread mqtt) >>= killThread
    hClose h

-- | Create a new connection with the same config. Does not terminate the old
-- connection.
reconnect :: MQTT -> IO ()
reconnect mqtt = for_ (cReconnPeriod $ config mqtt) $ \period -> do
    -- Other threads can't write while the MVar is empty
    _ <- takeMVar (handle mqtt)
    logMsg mqtt "Reconnecting..."
    -- Temporarily create a new MVar for the handshake so other threads
    -- don't write before the connection is fully established
    handleVar <- newEmptyMVar
    go period (mqtt { handle = handleVar })
    readMVar handleVar >>= putMVar (handle mqtt)
  where
    -- try reconnecting until it works
    go p mqtt' = do
        let conf = config mqtt
        connectTo (cHost conf) (PortNumber $ cPort conf)
          >>= putMVar (handle mqtt')
        logMsg mqtt' "Sending handshake"
        mCode <- handshake mqtt'
        unless (mCode == Just 0) $
          takeMVar (handle mqtt') >> go p mqtt'
      `catch`
        \e -> do
            logMsg mqtt $ show (e :: IOException)
            threadDelay (p * 10^6)
            go p mqtt'


-----------------------------------------
-- Internal
-----------------------------------------

handshake :: MQTT -> IO (Maybe Word8)
handshake mqtt = do
    h <- readMVar $ handle mqtt
    let timeout' = maybe (fmap Just) (timeout . (* 1000000))
                     (cConnectTimeout (config mqtt))
    sendConnect mqtt
    msg <- timeout' (getMessage h) `catch` \e ->
             Nothing <$ logMsg mqtt (show (e :: MQTTException) ++
                                      " while waiting for CONNACK")
    return $ case msg of
      Just (ConnAck code) -> Just code
      _ ->  Nothing

sendConnect :: MQTT -> IO ()
sendConnect mqtt = send mqtt connect
  where
    conf = config mqtt
    (willVH, willPL) = case cWill conf of
                         Just w  -> (Just (wQoS w, wRetain w)
                                    , Just (wTopic w, wMsg w))
                         Nothing -> (Nothing, Nothing)
    connect = Message
                (Header CONNECT False NoConfirm False)
                (Just (VHConnect (ConnectHeader
                                   "MQIsdp"
                                   3
                                   (cClean conf)
                                   willVH
                                   (isJust $ cUsername conf)
                                   (isJust $ cPassword conf)
                                   (maybe 0 fromIntegral $ cKeepAlive conf))))
                (Just (PLConnect (ConnectPL
                                   (MqttText $ cClientID conf)
                                   (fst <$> willPL)
                                   (MqttText . snd <$> willPL)
                                   (MqttText <$> cUsername conf)
                                   (MqttText <$> cPassword conf))))

recvLoop :: MQTT -> IO ()
recvLoop mqtt = forever $ do
    h <- readMVar (handle mqtt)
    eof <- hIsEOF h
    if eof
      then reconnect mqtt
      else do
        msg <- getMessage h
        hs <- M.lookup (msgType $ header msg) <$> readMVar (handlers mqtt)
        for_ hs $ mapM_ (forkIO . ($ msg) . snd)
  `catches`
    [ Handler $ \e -> do logMsg mqtt ("Caught " ++ show (e :: IOException))
                         reconnect mqtt
    , Handler $ \e -> logMsg mqtt ("Caught " ++ show (e :: MQTTException))
    ]

publishHandler :: MQTT -> Message -> IO ()
publishHandler mqtt msg@(Publish topic body) = do
    case msg of
      PubConfirm msgid -> send mqtt $
                  Message
                    (Header PUBACK False NoConfirm False)
                    (Just (VHOther msgid))
                    Nothing
      PubHandshake msgid -> do
          send mqtt $ Message
                        (Header PUBREC False NoConfirm False)
                        (Just (VHOther msgid))
                        Nothing
          awaitMsg mqtt PUBREL Nothing
          send mqtt $ Message
                        (Header PUBCOMP False NoConfirm False)
                        (Just (VHOther msgid))
                        Nothing
      _ -> return ()
    callbacks <- filter (matches topic . fst) <$> readMVar (topicHandlers mqtt)
    for_ callbacks $ \(_,f) -> f topic body
publishHandler mqtt _ = return ()

logMsg :: MQTT -> String -> IO ()
logMsg mqtt msg = putStrLn msg

getMessage :: Handle -> IO Message
getMessage h = do
    header <- hGet' h 1
    remaining <- getRemaining h 0
    rest <- hGet' h remaining
    let parseRslt = do
          h <- parseOnly mqttHeader header
          parseOnly (body h (fromIntegral remaining)) rest
    case parseRslt of
      Left err -> throw (ParseError err)
      Right msg -> return msg

getRemaining :: Handle -> Int -> IO Int
getRemaining h n = go n 1
  where
    go acc fac = do
      b <- getByte h
      let acc' = acc + (b .&. 127) * fac
      if b .&. 128 == 0
        then return acc'
        else go acc' (fac * 128)

getByte :: Handle -> IO Int
getByte h = fromIntegral . BS.head <$> hGet' h 1

hGet' :: Handle -> Int -> IO BS.ByteString
hGet' h n = do
    bs <- hGet h n
    if BS.length bs < n
      then throw EOF
      else return bs

-- | Exceptions that may arise while parsing messages. A user should
-- never see one of these.
data MQTTException
    = EOF
    | ParseError String
    deriving (Show, Typeable)

instance Exception MQTTException where
