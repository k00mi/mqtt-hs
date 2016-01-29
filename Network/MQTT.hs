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
>>> let f t payload = putStrLn $ "A message was published to " ++ show t ++ ": " ++ show payload
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
  , sendAwait
  , send
  , doAwait
  , doAwait'
  , addHandler
  , removeHandler
  -- * Reexports
  , module Network.MQTT.Types
  ) where

import Control.Applicative (pure, (<$>), (<*>), (<$))
import Control.Concurrent
import Control.Exception hiding (handle)
import Control.Monad hiding (sequence_)
import Data.Attoparsec.ByteString (parseOnly, endOfInput)
import Data.Bits ((.&.))
import Data.ByteString (hGet, ByteString)
import qualified Data.ByteString as BS
import Data.Foldable (for_, sequence_, traverse_)
import Data.Maybe (isJust, fromJust)
import Data.Singletons (SingI(..))
import Data.Singletons.Decide
import Data.Text (Text)
import Data.Traversable (for)
import Data.Typeable (Typeable)
import Data.Unique
import Data.Word
import Network
import Prelude hiding (sequence_)
import System.IO (Handle, hClose, hIsEOF, hSetBinaryMode)
import System.IO.Error (ioError, mkIOError, eofErrorType, annotateIOError)
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
        , sendSignal :: Maybe (MVar ()) -- Nothing if keep alive is 0
        }

data TopicHandler
    = TopicHandler
        { thTopic :: Topic
        , thHandler :: Topic -> ByteString -> IO ()
        }

data MessageHandler where
    MessageHandler :: SingI t
                   => Unique -> (Message t -> IO ()) -> MessageHandler

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


-- | Establish a connection. Might throw an 'IOException' or a 'ConnectError'.
connect :: MQTTConfig -> IO MQTT
connect conf = do
    h <- connectTo (cHost conf) (PortNumber $ cPort conf)
    mqtt <- mkMQTT conf h
    (mqtt <$ setup mqtt) `onException` disconnect mqtt

-- | Create an uninitialized 'MQTT'.
mkMQTT :: MQTTConfig -> Handle -> IO MQTT
mkMQTT conf h = MQTT conf
                  <$> newMVar h
                  <*> newMVar []
                  <*> newMVar []
                  <*> newEmptyMVar
                  <*> newEmptyMVar
                  <*> newEmptyMVar
                  <*> for (cKeepAlive conf) (const newEmptyMVar)

-- | Handshake with the broker and start threads.
setup :: MQTT -> IO ()
setup mqtt = do
    h <- readMVar (handle mqtt)
    hSetBinaryMode h True
    handshake mqtt
    addHandler mqtt (publishHandler mqtt)
    forkIO (recvLoop mqtt) >>= putMVar (recvThread mqtt)
    for_ (keepAliveLoop mqtt) $ putMVar (keepAliveThread mqtt) <=< forkIO

-- | Send a 'Message' to the server.
--
-- In the case of an 'IOException', a reconnect is initiated and the 'Message'
-- sent again if 'cReconnPeriod' is set; otherwise the exception is rethrown.
send :: SingI t => MQTT -> Message t -> IO ()
send mqtt msg = do
    logDebug mqtt $ "Sending " ++ show (toMsgType msg)
    h <- readMVar (handle mqtt)
    writeTo h msg
    for_ (sendSignal mqtt) $ flip tryPutMVar ()
  `catch`
    \e -> do
      logError mqtt $ "send: Caught " ++ show (e :: IOException)
      didReconnect <- maybeReconnect mqtt
      if didReconnect
         then send mqtt msg
         else throw e

handshake :: MQTT -> IO ()
handshake mqtt = do
    let timeout' = case cConnectTimeout (config mqtt) of
                     Nothing -> fmap Just
                     Just t -> timeout (t * 1000000)
    sendConnect mqtt
    mMsg <- timeout' (getMessage mqtt)
              `catch` \(_ :: MQTTException) -> throw InvalidResponse
    case mMsg of
      Just (SomeMessage (Message _ (ConnAck code))) ->
        when (code /= 0) $ throw (toConnectError code)
      Just _ -> throw InvalidResponse
      Nothing -> throw Timeout

sendConnect :: MQTT -> IO ()
sendConnect mqtt = send mqtt connect
  where
    conf = config mqtt
    connect = Message
                (Header False NoConfirm False)
                (Connect
                  (cClean conf)
                  (cWill conf)
                  (MqttText $ cClientID conf)
                  (MqttText <$> cUsername conf)
                  (MqttText <$> cPassword conf)
                  (maybe 0 fromIntegral $ cKeepAlive conf))

-- | Execute an @IO@ action and immediately wait for a response, avoiding
-- race conditions.
--
-- Note this expects a singleton to guarantee the returned 'Message' is of
-- the 'MsgType' that is being waited for. Singleton constructors are the
-- 'MsgType' constructors prefixed with a capital @S@, e.g. 'SPUBLISH'.
doAwait :: SingI t
        => MQTT -> IO () -> SMsgType t -> Maybe MsgID -> IO (Message t)
doAwait mqtt io _ mMsgID = do
    var <- newEmptyMVar
    bracket
      (addHandler mqtt (putMVar var))
      (removeHandler mqtt)
      (\handlerID ->
        let wait = do
              msg <- readMVar var
              if isJust mMsgID
                then if mMsgID == getMsgID (body msg)
                       then return msg
                       else wait
                else return msg
        in io >> wait)

-- | A version of 'doAwait' that infers the type of the 'Message' that
-- is expected.
doAwait' :: SingI t => MQTT -> IO () -> Maybe MsgID -> IO (Message t)
doAwait' mqtt io mMsgID = doAwait mqtt io sing mMsgID

-- | Execute the common pattern of sending a message and awaiting
-- a response in a safe, non-racy way.
sendAwait :: (SingI t, SingI r)
          => MQTT -> Message t -> SMsgType r -> Maybe MsgID -> IO (Message r)
sendAwait mqtt msg responseT mMsgID =
    doAwait mqtt (send mqtt msg) responseT mMsgID

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
      return $ TopicHandler topic handler : hs
    return qosGranted

sendSubscribe :: MQTT -> QoS -> Topic -> IO QoS
sendSubscribe mqtt qos topic = do
    msgID <- fromIntegral . hashUnique <$> newUnique
    msg <- sendAwait mqtt
             (Message
               (Header False Confirm False)
               (Subscribe msgID [(topic, qos)]))
             SSUBACK (Just msgID)
    case granted $ body $ msg of
      [qosGranted] -> return qosGranted
      _ -> fail $ "Received invalid message as response to subscribe: "
                    ++ show (toMsgType msg)

-- | Unsubscribe from the given 'Topic' and remove any handlers.
unsubscribe :: MQTT -> Topic -> IO ()
unsubscribe mqtt topic = do
    msgID <- fromIntegral . hashUnique <$> newUnique
    _ <- sendAwait mqtt
           (Message (Header False Confirm False) (Unsubscribe msgID [topic]))
           SUNSUBACK (Just msgID)
    modifyMVar_ (topicHandlers mqtt) $ return . filter ((/= topic) . thTopic)

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
    let pub = Message (Header False qos retain) (Publish topic msgID body)
    case qos of
      NoConfirm -> send mqtt pub
      Confirm   -> void $ sendAwait mqtt pub SPUBACK msgID
      Handshake -> do
        void $ sendAwait mqtt pub SPUBREC msgID
        void $ sendAwait mqtt
                 (Message (Header False Confirm False)
                          (PubRel (fromJust msgID)))
                 SPUBCOMP msgID

-- | Close the connection to the server.
disconnect :: MQTT -> IO ()
disconnect mqtt = mask_ $ do
    tryReadMVar (recvThread mqtt) >>= traverse_ killThread
    tryReadMVar (keepAliveThread mqtt) >>= traverse_ killThread
    h <- takeMVar $ handle mqtt
    writeTo h (Message (Header False NoConfirm False) Disconnect)
      `catch` \e -> do
        logWarning mqtt $ show $
          annotateIOError e "disconnect/writeTo" (Just h) Nothing
        logWarning mqtt "Continuing disconnect."
    hClose h
      `catch` \e -> do
        logWarning mqtt $ show $
          annotateIOError e "disconnect/hClose" (Just h) Nothing
        logWarning mqtt "Continuing disconnect."
    logInfo mqtt "Disconnected."

-- | Try creating a new connection with the same config (retrying after the
-- specified amount of seconds has passed) and invoke the callback that is
-- set with 'onReconnect' once a new connection has been established.
--
-- Does not terminate the old connection.
reconnect :: MQTT -> Int -> IO ()
reconnect mqtt period = do
    -- Other threads can't write while the MVar is empty
    mh <- tryTakeMVar (handle mqtt)
    -- If it was empty, someone else is already reconnecting
    for_ mh $ \_ -> do
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
        handshake mqtt'
      `catches`
        let logAndRetry :: Exception e => e -> IO ()
            logAndRetry e = do
              mh <- tryTakeMVar (handle mqtt')
              for mh $ \h -> hClose h `catch` \(e :: IOException) -> return ()
              logWarning mqtt $ "reconnect: " ++ show e
              threadDelay (period * 10^6)
              go mqtt'
        in [ Handler $ \e -> logAndRetry (e :: IOException)
           , Handler $ \e -> logAndRetry (e :: ConnectError)
           ]

-- | Register a callback that will be invoked when a reconnect has
-- happened.
onReconnect :: MQTT -> IO () -> IO ()
onReconnect mqtt io = do
    let mvar = reconnectHandler mqtt
    _ <- tryTakeMVar mvar
    putMVar mvar io

-- | Close the handle and initiate a reconnect, if 'cReconnPeriod' is set.
maybeReconnect :: MQTT -> IO Bool
maybeReconnect mqtt = do
    catch
      (readMVar (handle mqtt) >>= hClose)
      (const (pure ()) :: IOException -> IO ())
    maybe (return False) (fmap  (const True) . reconnect mqtt) $
      cReconnPeriod (config mqtt)


-----------------------------------------
-- Logger utility functions
-----------------------------------------

logDebug :: MQTT -> String -> IO ()
logDebug mqtt = L.logDebug (cLogger (config mqtt))

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
recvLoop m = loopWithReconnect m "recvLoop" $ \mqtt -> do
    h <- readMVar (handle mqtt)
    eof <- hIsEOF h
    if eof
      then ioError $ mkIOError eofErrorType "" (Just h) Nothing
      else getMessage mqtt >>= dispatchMessage mqtt
  `catch`
    \e -> logWarning mqtt $ "recvLoop: Caught " ++ show (e :: MQTTException)

dispatchMessage :: MQTT -> SomeMessage -> IO ()
dispatchMessage mqtt (SomeMessage msg) =
    readMVar (handlers mqtt) >>= mapM_ applyMsg
  where
    applyMsg :: MessageHandler -> IO ()
    applyMsg (MessageHandler _ (handler :: Message t' -> IO ())) =
      case toSMsgType msg %~ (sing :: SMsgType t') of
        Proved Refl -> void $ forkIO $ handler msg
        Disproved _ -> return ()

-- | Returns 'Nothing' if no Keep Alive is specified.
-- Otherwise returns a loop that blocks on an @MVar ()@ that is written to by
-- 'send'. If the Keep Alive period runs out while waiting, send a 'PINGREQ'
-- to the server and wait for PINGRESP.
keepAliveLoop :: MQTT -> Maybe (IO ())
keepAliveLoop mqtt = do
    period <- cKeepAlive (config mqtt)
    sig <- sendSignal mqtt
    return $ loopWithReconnect mqtt "keepAliveLoop" (loopBody period sig)
  where
    loopBody period mvar mqtt = do
      rslt <- timeout (period * 1000000) $ takeMVar mvar
      case rslt of
        Nothing -> void $ sendAwait mqtt
                     (Message (Header False NoConfirm False) PingReq)
                     SPINGRESP Nothing
        Just _ -> return ()

-- | Forever executes the given @IO@ action. In the case of an 'IOException',
-- a reconnect is initiated if 'cReconnPeriod' is set, otherwise the action
-- terminates.
loopWithReconnect :: MQTT -> String -> (MQTT -> IO ()) -> IO ()
loopWithReconnect mqtt desc act = catch (forever $ act mqtt) $ \e -> do
  logError mqtt $ show $ annotateIOError e desc Nothing Nothing
  didReconnect <- maybeReconnect mqtt
  if didReconnect
    then loopWithReconnect mqtt desc act
    else logError mqtt $ desc ++ ": No reconnect, terminating."

publishHandler :: MQTT -> Message PUBLISH -> IO ()
publishHandler mqtt (Message header body) = do
    case (qos header, pubMsgID body) of
      (Confirm, Just msgid) ->
          send mqtt $ Message (Header False NoConfirm False) (PubAck msgid)
      (Handshake, Just msgid) -> do
          sendAwait mqtt
            (Message (Header False NoConfirm False) (PubRec msgid))
            SPUBREL (Just msgid)
          send mqtt $ Message (Header False NoConfirm False) (PubComp msgid)
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
          (mType, header) <- parseOnly (mqttHeader <* endOfInput) headerByte
          withSomeSingI mType $ \sMsgType ->
            parseOnly
              (SomeMessage . Message header
                <$> mqttBody header sMsgType (fromIntegral remaining)
                <* endOfInput)
              rest
    case parseRslt of
      Left err -> logError mqtt ("Error while parsing: " ++ err) >>
                  throw (ParseError err)
      Right msg -> msg <$
        logDebug mqtt ("Received " ++ show (toMsgType' msg))

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
