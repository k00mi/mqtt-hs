{-# Language OverloadedStrings,
             DataKinds,
             ScopedTypeVariables,
             GADTs,
             DeriveDataTypeable #-}
{-|
Module: MQTT
Copyright: Lukas Braun 2014
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

A MQTT client library.

A simple example, assuming a broker is running on localhost
(needs -XOverloadedStrings):

>>> import Network.MQTT
>>> import Network.MQTT.Logger
>>> Just mqtt <- connect defaultConfig { cLogger = warnings stdLogger }
>>> let f t payload = putStrLn $ "A message was published to " ++ show t ++ ": " ++ show pyload
>>> subscribe mqtt NoConfirm "#" f
NoConfirm
>>> publish mqtt Handshake False "some random/topic" "Some content!"
A message was published to "some random/topic": "Some content!"
-}
module Network.MQTT
  ( -- * Creating connections
    connect
  , MQTT
  , disconnect
  , reconnect
  , onReconnect
  , resubscribe
  -- * Connection settings
  , MQTTConfig
  , defaultConfig
  -- ** Field accessors
  , cHost
  , cPort
  , cClean
  , cWill
  , cUsername
  , cPassword
  , cKeepAlive
  , cClientID
  , cConnectTimeout
  , cReconnPeriod
  , cLogger
  -- * Subscribing and publishing
  , subscribe
  , unsubscribe
  , publish
  -- * Sending and receiving 'Message's
  , send
  , addHandler
  , removeHandler
  , awaitMsg
  , awaitMsg'
  -- * Reexports
  , module Network.MQTT.Types
  ) where

import Control.Applicative (pure, (<$>), (<*>), (<$))
import Control.Concurrent
import Control.Exception hiding (handle)
import Control.Monad hiding (sequence_)
import Data.Attoparsec (parseOnly)
import Data.Bits ((.&.))
import Data.ByteString (hGet, ByteString)
import qualified Data.ByteString as BS
import Data.Foldable (for_, sequence_, traverse_)
import Data.Maybe (isJust, fromJust)
import Data.Singletons (withSomeSing, SingI(..))
import Data.Singletons.Decide
import Data.Text (Text)
import Data.Traversable (for)
import Data.Typeable (Typeable)
import Data.Unique
import Data.Word
import Network
import Prelude hiding (sequence_)
import System.IO (Handle, hClose, hIsEOF, hSetBinaryMode)
import System.Timeout (timeout)

import Network.MQTT.Types
import Network.MQTT.Parser (mqttBody, mqttHeader)
import Network.MQTT.Encoding
import qualified Network.MQTT.Logger as L

-----------------------------------------
-- Interface
-----------------------------------------

-- | Abstract type representing a connection to a broker.
data MQTT
    = MQTT
        { config :: MQTTConfig
        , handle :: MVar Handle
        , handlers :: MVar [MessageHandler]
        , topicHandlers :: MVar [TopicHandler]
        , recvThread :: MVar ThreadId
        , reconnectHandler :: MVar (IO ())
        , keepAliveThread :: MVar ThreadId
        , sendSem :: Maybe QSem
        }

data TopicHandler
    = TopicHandler
        { thTopic :: Topic
        , thQoS :: QoS
        , thHandler :: Topic -> ByteString -> IO ()
        }

data MessageHandler where
    MessageHandler :: SingI t
                   => Unique
                   -> (Message t -> IO ())
                   -> MessageHandler

-- | The various options when establishing a connection.
data MQTTConfig
    = MQTTConfig
        { cHost :: HostName
        -- ^ Hostname of the broker.
        , cPort :: PortNumber
        -- ^ Port of the broker.
        , cClean :: Bool
        -- ^ Should the server forget subscriptions and other state on
        -- disconnects?
        , cWill :: Maybe Will
        -- ^ Optional 'Will' message.
        , cUsername :: Maybe Text
        -- ^ Optional username used for authentication.
        , cPassword :: Maybe Text
        -- ^ Optional password used for authentication.
        , cKeepAlive :: Maybe Int
        -- ^ Maximum interval (in seconds) in which a message must be sent.
        -- 0 means no limit.
        , cClientID :: Text
        -- ^ Client ID used by the server to identify clients.
        , cConnectTimeout :: Maybe Int
        -- ^ Time in seconds after which waiting for a CONNACK is aborted.
        -- 'Nothing' means no timeout.
        , cReconnPeriod :: Maybe Int
        -- ^ Time in seconds to wait between reconnect attempts.
        -- 'Nothing' means no reconnects are attempted.
        , cLogger :: L.Logger
        -- ^ Functions for logging, see 'Network.MQTT.Logger.Logger'.
        }

-- | Defaults for 'MQTTConfig', connects to a server running on
-- localhost.
defaultConfig :: MQTTConfig
defaultConfig = MQTTConfig
    { cHost             = "localhost"
    , cPort             = 1883
    , cClean            = True
    , cWill             = Nothing
    , cUsername         = Nothing
    , cPassword         = Nothing
    , cKeepAlive        = Nothing
    , cClientID         = "mqtt-haskell"
    , cConnectTimeout   = Nothing
    , cReconnPeriod     = Nothing
    , cLogger           = L.stdLogger
    }


-- | Establish a connection. This might fail with an 'IOException' or
-- return 'Nothing' if the server did not accept the connection.
connect :: MQTTConfig -> IO (Maybe MQTT)
connect conf = do
    h <- connectTo (cHost conf) (PortNumber $ cPort conf)
    hSetBinaryMode h True
    mqtt <- MQTT conf
              <$> newMVar h
              <*> newMVar []
              <*> newMVar []
              <*> newEmptyMVar
              <*> newEmptyMVar
              <*> newEmptyMVar
              <*> for (cKeepAlive conf) (const (newQSem 0))
    mCode <- handshake mqtt
    if mCode == Just 0
      then Just mqtt <$ do forkIO (recvLoop mqtt) >>= putMVar (recvThread mqtt)
                           forkIO (keepAliveLoop mqtt) >>=
                             putMVar (keepAliveThread mqtt)
                           addHandler mqtt (publishHandler mqtt)
      else Nothing <$ hClose h

-- | Send a 'Message' to the server.
send :: MQTT -> Message t -> IO ()
send mqtt msg = do
    logInfo mqtt $ "Sending " ++ show (toMsgType msg)
    h <- readMVar (handle mqtt)
    writeTo h msg
    for_ (sendSem mqtt) signalQSem

handshake :: MQTT -> IO (Maybe Word8)
handshake mqtt = do
    let timeout' = maybe (fmap Just) (timeout . (* 1000000))
                     (cConnectTimeout (config mqtt))
    sendConnect mqtt
    msg <- timeout' (getMessage mqtt) `catch` \e ->
             Nothing <$ logError mqtt (show (e :: MQTTException) ++
                                      " while waiting for CONNACK")
    return $ case msg of
      Just (SomeMessage (Message _ (MConnAck (ConnAck code)))) -> Just code
      _ ->  Nothing

sendConnect :: MQTT -> IO ()
sendConnect mqtt = send mqtt connect
  where
    conf = config mqtt
    connect = Message
                (Header False NoConfirm False)
                (MConnect $ Connect
                  (cClean conf)
                  (cWill conf)
                  (MqttText $ cClientID conf)
                  (MqttText <$> cUsername conf)
                  (MqttText <$> cPassword conf)
                  (maybe 0 fromIntegral $ cKeepAlive conf))

-- | Block until a 'Message' of the given type, optionally with the given
-- 'MsgID', arrives.
--
-- Note this expects a singleton to guarantee the returned 'Message' is of
-- the 'MsgType' that is being waited for. Singleton constructors are the
-- 'MsgType' constructors prefixed with a capital @S@, e.g. 'SPUBLISH'.
awaitMsg :: SingI t => MQTT -> SMsgType t -> Maybe MsgID -> IO (Message t)
awaitMsg mqtt _ mMsgID = do
    var <- newEmptyMVar
    handlerID <- addHandler mqtt (putMVar var)
    let wait = do
          msg <- readMVar var
          if isJust mMsgID
            then if mMsgID == getMsgID (body msg)
                   then removeHandler mqtt handlerID >> return msg
                   else wait
            else removeHandler mqtt handlerID >> return msg
    wait

-- | A version of 'awaitMsg' that infers the type of the 'Message' that
-- is expected.
awaitMsg' :: SingI t => MQTT -> Maybe MsgID -> IO (Message t)
awaitMsg' mqtt mMsgID = awaitMsg mqtt sing mMsgID

-- | Register a callback that gets invoked whenever a 'Message' of the
-- expected 'MsgType' is received. Returns the ID of the handler which can be
-- passed to 'removeHandler'.
addHandler :: SingI t => MQTT -> (Message t -> IO ()) -> IO Unique
addHandler mqtt handler = do
    mhID <- newUnique
    modifyMVar_ (handlers mqtt) $ \hs ->
      return $ MessageHandler mhID handler : hs
    return mhID

-- | Remove the handler with the given ID.
removeHandler :: MQTT -> Unique -> IO ()
removeHandler mqtt mhID = modifyMVar_ (handlers mqtt) $ \hs ->
    return $ filter (\(MessageHandler mhID' _) -> mhID' /= mhID) hs

-- | Subscribe to a 'Topic' with the given 'QoS' and invoke the callback
-- whenever something is published to the 'Topic'. Returns the 'QoS' that
-- was granted by the broker (lower or equal to the one requested).
--
-- The 'Topic' may contain
-- <http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#appendix-a wildcars>.
-- The 'Topic' passed to the callback is the fully expanded version where
-- the message was actually published.
subscribe :: MQTT -> QoS -> Topic -> (Topic -> ByteString -> IO ())
          -> IO QoS
subscribe mqtt qos topic handler = do
    qosGranted <- sendSubscribe mqtt qos topic
    modifyMVar_ (topicHandlers mqtt) $ \hs ->
      return $ TopicHandler topic qosGranted handler : hs
    return qosGranted

sendSubscribe :: MQTT -> QoS -> Topic -> IO QoS
sendSubscribe mqtt qos topic = do
    msgID <- fromIntegral . hashUnique <$> newUnique
    send mqtt $ Message
                  (Header False Confirm False)
                  (MSubscribe $ Subscribe
                    msgID
                    [(topic, qos)])
    msg <- awaitMsg mqtt SSUBACK (Just msgID)
    case msg of
      (Message _ (MSubAck (SubAck _ [qosGranted]))) -> return qosGranted
      _ -> fail $ "Received invalid message as response to subscribe: "
                    ++ show (toMsgType msg)

-- | Unsubscribe from the given 'Topic' and remove any handlers.
unsubscribe :: MQTT -> Topic -> IO ()
unsubscribe mqtt topic = do
    modifyMVar_ (topicHandlers mqtt) $ return . filter ((== topic) . thTopic)
    msgID <- fromIntegral . hashUnique <$> newUnique
    send mqtt $ Message
                  (Header False Confirm False)
                  (MUnsubscribe $ Unsubscribe msgID [topic])
    void $ awaitMsg mqtt SUNSUBACK (Just msgID)

-- | Publish a message to the given 'Topic' at the requested 'QoS' level.
-- The payload can be any sequence of bytes, including none at all. The 'Bool'
-- parameter decides if the server should retain the message for future
-- subscribers to the topic.
--
-- The 'Topic' must not contain
-- <http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#appendix-a wildcards>.
publish :: MQTT -> QoS -> Bool -> Topic -> ByteString -> IO ()
publish mqtt qos retain topic body = do
    msgID <- if qos > NoConfirm
               then Just . fromIntegral . hashUnique <$> newUnique
               else return Nothing
    send mqtt $ Message
                  (Header False qos retain)
                  (MPublish $ Publish topic msgID body)
    case qos of
      NoConfirm -> return ()
      Confirm   -> void $ awaitMsg mqtt SPUBACK msgID
      Handshake -> do
        void $ awaitMsg mqtt SPUBREC msgID
        send mqtt $ Message
                      (Header False Confirm False)
                      (MPubRel $ SimpleMsg (fromJust msgID))
        void $ awaitMsg mqtt SPUBCOMP msgID

-- | Close the connection to the server.
disconnect :: MQTT -> IO ()
disconnect mqtt = do
    h <- takeMVar $ handle mqtt
    writeTo h $
      Message
        (Header False NoConfirm False)
        MDisconnect
    readMVar (recvThread mqtt) >>= killThread
    readMVar (keepAliveThread mqtt) >>= killThread
    hClose h

-- | Try creating a new connection with the same config (retrying after the
-- specified amount of seconds has passed) and invoke the callback that is
-- set with 'onReconnect' once a new connection has been established.
--
-- Does not terminate the old connection.
reconnect :: MQTT -> Int -> IO ()
reconnect mqtt period = do
    -- Other threads can't write while the MVar is empty
    _ <- takeMVar (handle mqtt)
    logInfo mqtt "Reconnecting..."
    -- Temporarily create a new MVar for the handshake so other threads
    -- don't write before the connection is fully established
    handleVar <- newEmptyMVar
    go (mqtt { handle = handleVar })
    readMVar handleVar >>= putMVar (handle mqtt)
    -- forkIO so recvLoop isn't blocked
    tryReadMVar (reconnectHandler mqtt) >>= traverse_ (void . forkIO)
    logInfo mqtt "Reconnect successfull"
  where
    -- try reconnecting until it works
    go mqtt' = do
        let conf = config mqtt
        connectTo (cHost conf) (PortNumber $ cPort conf)
          >>= putMVar (handle mqtt')
        mCode <- handshake mqtt'
        unless (mCode == Just 0) $ do
          void $ takeMVar (handle mqtt')
          threadDelay (period * 10^6)
          go mqtt'
      `catch`
        \e -> do
            logWarning mqtt $ "reconnect: " ++ show (e :: IOException)
            threadDelay (period * 10^6)
            go mqtt'

-- | Register a callback that will be invoked when a reconnect has
-- happened.
onReconnect :: MQTT -> IO () -> IO ()
onReconnect mqtt io = do
    let mvar = reconnectHandler mqtt
    empty <- isEmptyMVar mvar
    unless empty (void $ takeMVar mvar)
    putMVar mvar io

-- | Resubscribe to all topics. Returns the new list of granted 'QoS'.
resubscribe :: MQTT -> IO [QoS]
resubscribe mqtt = do
    ths <- readMVar (topicHandlers mqtt)
    mapM (\th -> sendSubscribe mqtt (thQoS th) (thTopic th)) ths

maybeReconnect :: MQTT -> IO ()
maybeReconnect mqtt = do
    catch
      (readMVar (handle mqtt) >>= hClose)
      (const (pure ()) :: IOException -> IO ())
    for_ (cReconnPeriod $ config mqtt) $ reconnect mqtt


-----------------------------------------
-- Logger utility functions
-----------------------------------------

logInfo :: MQTT -> String -> IO ()
logInfo mqtt = L.logInfo (cLogger (config mqtt))

logWarning :: MQTT -> String -> IO ()
logWarning mqtt = L.logWarning (cLogger (config mqtt))

logError :: MQTT -> String -> IO ()
logError mqtt = L.logError (cLogger (config mqtt))


-----------------------------------------
-- Internal
-----------------------------------------

recvLoop :: MQTT -> IO ()
recvLoop mqtt = forever $ do
    h <- readMVar (handle mqtt)
    eof <- hIsEOF h
    if eof
      then do
        logError mqtt "EOF in recvLoop"
        maybeReconnect mqtt
      else getMessage mqtt >>= dispatchMessage mqtt
  `catches`
    [ Handler $ \e -> do
        logError mqtt $ "recvLoop: Caught " ++ show (e :: IOException)
        maybeReconnect mqtt
    , Handler $ \e ->
        logWarning mqtt $ "recvLoop: Caught " ++ show (e :: MQTTException)
    ]

dispatchMessage :: MQTT -> SomeMessage -> IO ()
dispatchMessage mqtt (SomeMessage (msg :: Message t)) =
    readMVar (handlers mqtt) >>= mapM_ applyMsg
  where
    typeSing :: SMsgType t
    typeSing = toSMsgType msg

    applyMsg :: MessageHandler -> IO ()
    applyMsg (MessageHandler _ (handler :: Message t' -> IO ())) =
      case typeSing %~ (sing :: SMsgType t') of
        Proved Refl -> void $ forkIO $ handler msg
        Disproved _ -> return ()

-- | Block on a semaphore that is signaled by 'send'. If a timeout occurs
-- while waiting, send a 'PINGREQ' to the server and wait for PINGRESP.
-- Ignores errors that occur while writing to the handle, reconnects are
-- initiated by 'recvLoop'.
--
-- Returns immediately if no Keep Alive is specified.
keepAliveLoop :: MQTT -> IO ()
keepAliveLoop mqtt =
    sequence_ (loop <$> cKeepAlive (config mqtt) <*> sendSem mqtt)
  where
    loop period sem = forever $ do
      rslt <- timeout (period * 1000000) $ waitQSem sem
      case rslt of
        Nothing -> (do send mqtt $ Message
                                    (Header False NoConfirm False)
                                    MPingReq
                       void $ awaitMsg mqtt SPINGRESP Nothing)
                  `catch`
                    (\e -> logError mqtt $ "keepAliveLoop: " ++ show (e :: IOException))
        Just _ -> return ()

publishHandler :: MQTT -> Message PUBLISH -> IO ()
publishHandler mqtt (Message header (MPublish body)) = do
    case (qos header, pubMsgID body) of
      (Confirm, Just msgid) ->
          send mqtt $ Message
                        (Header False NoConfirm False)
                        (MPubAck $ SimpleMsg msgid)
      (Handshake, Just msgid) -> do
          send mqtt $ Message
                        (Header False NoConfirm False)
                        (MPubRec $ SimpleMsg msgid)
          void $ awaitMsg mqtt SPUBREL (Just msgid)
          send mqtt $ Message
                        (Header False NoConfirm False)
                        (MPubComp $ SimpleMsg msgid)
      _ -> return ()
    callbacks <- filter (matches (topic body) . thTopic)
                   <$> readMVar (topicHandlers mqtt)
    for_ callbacks $ \th -> thHandler th (topic body) (payload body)

getMessage :: MQTT -> IO SomeMessage
getMessage mqtt = do
    h <- readMVar (handle mqtt)
    headerByte <- hGet' h 1
    remaining <- getRemaining h 0
    rest <- hGet' h remaining
    let parseRslt = do
          (mType, header) <- parseOnly mqttHeader headerByte
          withSomeSing mType $ \sMsgType ->
            parseOnly
              (SomeMessage . Message header
                <$> mqttBody header sMsgType (fromIntegral remaining))
              rest
    case parseRslt of
      Left err -> logError mqtt ("Error while parsing: " ++ err) >>
                  throw (ParseError err)
      Right msg -> msg <$
        logInfo mqtt ("Received " ++ show (toMsgType' msg))

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
