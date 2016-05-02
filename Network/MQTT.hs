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
>>> mqtt <- connect defaultConfig { cLogger = warnings stdLogger }
>>> let f (t, payload) = "A message was published to " ++ show t ++ ": " ++ show payload
>>> subscribe mqtt NoConfirm "#"
NoConfirm
>>> publish mqtt Handshake False "some random/topic" "Some content!"
>>> f <$> atomically (readTChan (events mqtt))
A message was published to "some random/topic": "Some content!"
-}
module Network.MQTT
  ( -- * Creating connections
    MQTT
  , connect
  , disconnect
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
  , events
  , Event(..)
  , Terminated(..)
  , publish
  -- * Reexports
  , module Network.MQTT.Types
  ) where

import Control.Applicative ((<$>), (<*>))
import Control.Concurrent
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.STM
import Control.Exception (catch, bracketOnError, IOException)
import Control.Monad (void, forever, filterM)
import Control.Monad.IO.Class (liftIO, MonadIO)
import Control.Monad.Loops (untilJust)
import Control.Monad.State.Strict (evalStateT, gets, modify, StateT)
import Data.Attoparsec.ByteString (IResult(..) , parse)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Foldable (for_)
import Data.Maybe (isNothing, fromJust)
import Data.Singletons (SingI(..))
import Data.Singletons.Decide
import Data.Text (Text)
import Data.Traversable (for)
import Data.Unique
import Network
import System.IO (Handle, hSetBinaryMode, hLookAhead)
import System.Timeout (timeout)

import Network.MQTT.Types
import Network.MQTT.Parser (message)
import Network.MQTT.Encoding (writeTo)
import qualified Network.MQTT.Logger as L

-----------------------------------------
-- Interface
-----------------------------------------

-- | Abstract type representing a connection to a broker.
data MQTT
    = MQTT
        { config :: MQTTConfig
        , events :: TChan Event
        , commands :: TChan Command
        , sendSignal :: Maybe (MVar ()) -- Nothing if keep alive is 0
        , mqttAsync :: MVar (Async.Async ())
        }

-- | Events for you, my dear user.
data Event
    = Published (Message PUBLISH)
    | Terminated Terminated
    -- ^ The connection has been terminated.
    -- No new messages will be written to the channel.

-- | Reasons for why the connection was terminated.
data Terminated
    = ParseFailed [String] String
    -- ^ at the context in @['String']@ with the given message.
    | IOErr IOException
    | ConnectFailed ConnectError
    | UserRequested
    deriving Show

-- | Commands from the user for the library.
data Command
    = CmdDisconnect
    | CmdSend SomeMessage
    | CmdAwait AwaitMessage
    | CmdStopWaiting AwaitMessage

data AwaitMessage where
    AwaitMessage :: SingI t => MVar (Message t) -> Maybe MsgID -> AwaitMessage

instance Eq AwaitMessage where
  AwaitMessage (var :: MVar (Message t)) mMsgID == AwaitMessage (var' :: MVar (Message t')) mMsgID' =
    case (sing :: SMsgType t) %~ (sing :: SMsgType t') of
      Proved Refl -> mMsgID == mMsgID' && var == var'
      Disproved _ -> False

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
        , cResendTimeout :: Int
        -- ^ Time in microseconds after which messages that have not been but
        -- should be acknowledged are retransmitted.
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
    , cResendTimeout    = 20 * 10^6 -- 20 seconds
    , cReconnPeriod     = Nothing
    , cLogger           = L.stdLogger
    }


-- | Establish a connection. Might throw an 'IOException'.
connect :: MQTTConfig -> IO MQTT
connect conf = do
    h <- connectTo (cHost conf) (PortNumber $ cPort conf)
    hSetBinaryMode h True
    mkMQTT conf h -- `onException` disconnect mqtt

-- | Start all the threads.
mkMQTT :: MQTTConfig -> Handle -> IO MQTT
mkMQTT conf h = do
    mqtt <- MQTT conf
              <$> newTChanIO
              <*> newTChanIO
              <*> for (cKeepAlive conf) (const newEmptyMVar)
              <*> newEmptyMVar
    Async.async (mainLoop mqtt h) >>= putMVar (mqttAsync mqtt)
    return mqtt

disconnect :: MQTT -> IO ()
disconnect mqtt = writeCmd mqtt CmdDisconnect

-- | Tell the `MQTT` instance to send the given `Message`.
-- Any errors that may happen while sending will come up as an `Event` on the
-- `events` channel.
send :: SingI t => MQTT -> Message t -> IO ()
send mqtt = writeCmd mqtt . CmdSend . SomeMessage

-- | Tell the `MQTT` instance to place the next `Message` of correct
-- `MsgType` and `MsgID` (if present) into the `MVar`.
registerVar :: SingI t => MQTT -> MVar (Message t) -> Maybe MsgID -> IO ()
registerVar mqtt var = writeCmd mqtt . CmdAwait . AwaitMessage var

-- | Stop waiting for the described `Message`.
unregisterVar :: SingI t => MQTT -> MVar (Message t) -> Maybe MsgID -> IO ()
unregisterVar mqtt var = writeCmd mqtt . CmdStopWaiting . AwaitMessage var

-- | Execute the common pattern of sending a message and awaiting
-- a response in a safe, non-racy way. The message message is retransmitted
-- if no response has been received after 'cResendTimeout' microseconds.
-- Exponential backoff is used for further retransmissions
-- An incoming message is considered a response if it is of the
-- requested type and the 'MsgID's match (if present).
--
-- Note this expects a singleton to guarantee the returned 'Message' is of
-- the 'MsgType' that is being waited for. Singleton constructors are the
-- 'MsgType' constructors prefixed with a capital @S@, e.g. 'SPUBLISH'.
sendAwait :: (SingI t, SingI r)
          => MQTT -> Message t -> SMsgType r -> IO (Message r)
sendAwait mqtt msg _responseS = do
    var <- newEmptyMVar
    let mMsgID = getMsgID (body msg)
    bracketOnError
      (registerVar mqtt var mMsgID)
      (\_ -> unregisterVar mqtt var mMsgID)
      (\_ ->
        let wait = do
              msg <- readMVar var
              if isNothing mMsgID || mMsgID == getMsgID (body msg)
                then return msg
                else wait
            keepTrying msg' tout = do
              send mqtt msg
              let retry = do
                    logInfo mqtt "No response within timeout, retransmitting..."
                    keepTrying (setDup msg') (tout * 2)
              timeout tout wait >>= maybe retry return
        in keepTrying msg (cResendTimeout $ config mqtt))

-- | Subscribe to a 'Topic' with the given 'QoS'. Returns the 'QoS' that was
-- granted by the broker (lower or equal to the one requested).
--
-- The 'Topic' may contain
-- <http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#appendix-a wildcars>.
subscribe :: MQTT -> QoS -> Topic -> IO QoS
subscribe mqtt qos topic = do
    msgID <- fromIntegral . hashUnique <$> newUnique
    msg <- sendAwait mqtt
             (Message
               (Header False Confirm False)
               (Subscribe msgID [(topic, qos)]))
             SSUBACK
    case granted $ body $ msg of
      [qosGranted] -> return qosGranted
      _ -> fail $ "Received invalid message as response to subscribe: "
                    ++ show (toMsgType msg)

-- | Unsubscribe from the given 'Topic'.
unsubscribe :: MQTT -> Topic -> IO ()
unsubscribe mqtt topic = do
    msgID <- fromIntegral . hashUnique <$> newUnique
    void $ sendAwait mqtt
           (Message (Header False Confirm False) (Unsubscribe msgID [topic]))
           SUNSUBACK

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
      Confirm   -> void $ sendAwait mqtt pub SPUBACK
      Handshake -> do
        void $ sendAwait mqtt pub SPUBREC
        void $ sendAwait mqtt
                 (Message (Header False Confirm False)
                          (PubRel (fromJust msgID)))
                 SPUBCOMP


-----------------------------------------
-- Logger utility functions
-----------------------------------------

logDebug :: MonadIO io => MQTT -> String -> io ()
logDebug mqtt = liftIO . L.logDebug (cLogger (config mqtt))

logInfo :: MonadIO io => MQTT -> String -> io ()
logInfo mqtt = liftIO . L.logInfo (cLogger (config mqtt))

logWarning :: MonadIO io => MQTT -> String -> io ()
logWarning mqtt = liftIO . L.logWarning (cLogger (config mqtt))

logError :: MonadIO io => MQTT -> String -> io ()
logError mqtt = liftIO . L.logError (cLogger (config mqtt))


-----------------------------------------
-- Internal
-----------------------------------------

type ParseCC = ByteString -> IResult ByteString SomeMessage

-- | Internal state for the main loop
data MqttState
  = MqttState
    { msParseCC :: ParseCC -- ^ Current parser continuation
    , msUnconsumed :: BS.ByteString -- ^ Not yet parsed input
    , msWaiting :: [AwaitMessage] -- ^ Messages we're waiting for
    }

-- | Input for the main loop
data Input
    = InMsg SomeMessage
    | InErr Terminated
    | InCmd Command

inputBufferSize :: Int
inputBufferSize = 4048

mainLoop :: MQTT -> Handle -> IO ()
mainLoop mqtt h = do
    for_ (sendSignal mqtt) $ forkMQTT mqtt . keepAliveLoop mqtt
    evalStateT
      (handshake >>=
        maybe (logDebug mqtt "Connected" >> go) (liftIO . terminate))
      (MqttState (parse message) BS.empty [])
  `catch`
    \e -> terminate $ IOErr e
  where
    go = do
      input <- waitForInput mqtt h
      case input of
        InErr err -> liftIO $
          terminate err
        InMsg someMsg -> do
          logDebug mqtt $ "Received " ++ show (toMsgType' someMsg)
          handleMessage mqtt someMsg
          go
        InCmd cmd -> case cmd of
          CmdDisconnect -> liftIO $ do
            doSend msgDisconnect
            terminate UserRequested
          CmdSend (SomeMessage msg) -> do
            doSend msg
            go
          CmdAwait awaitMsg -> do
            modify $ \s -> s { msWaiting = awaitMsg : msWaiting s }
            go
          CmdStopWaiting awaitMsg -> do
            modify $ \s -> s { msWaiting = filter (== awaitMsg) $ msWaiting s }
            go

    handshake :: StateT MqttState IO (Maybe Terminated)
    handshake = do
        doSend msgConnect
        input <- untilJust
                  (liftIO (BS.hGetSome h inputBufferSize) >>= parseBytes)
        case input of
          InErr err -> return $ Just err
          InMsg someMsg -> return $ case someMsg of
            SomeMessage (Message _ (ConnAck retCode)) ->
              if retCode /= 0
                then Just $ ConnectFailed $ toConnectError retCode
                else Nothing
            _ -> Just $ ConnectFailed InvalidResponse
          InCmd _ -> error "parseBytes returned InCmd, this should not happen."
      where
        conf = config mqtt
        msgConnect = Message
                    (Header False NoConfirm False)
                    (Connect
                      (cClean conf)
                      (cWill conf)
                      (MqttText $ cClientID conf)
                      (MqttText <$> cUsername conf)
                      (MqttText <$> cPassword conf)
                      (maybe 0 fromIntegral $ cKeepAlive conf))

    terminate = writeEvent mqtt . Terminated
    msgDisconnect = Message (Header False NoConfirm False) Disconnect

    doSend :: (MonadIO io, SingI t) => Message t -> io ()
    doSend msg = liftIO $ do
        logDebug mqtt $ "Sending " ++ show (toMsgType msg)
        writeTo h msg
        for_ (sendSignal mqtt) $ flip tryPutMVar ()

waitForInput :: MQTT -> Handle -> StateT MqttState IO Input
waitForInput mqtt h = do
    let cmdChan = commands mqtt
    unconsumed <- gets msUnconsumed
    if BS.null unconsumed
      then do
        -- wait until input is available, but don't retrieve it yet to avoid
        -- loosing anything
        input <- liftIO $ Async.race
                  (void $ hLookAhead h)
                  (void $ atomically $ peekTChan cmdChan)
        -- now we have committed to one source and can actually read it
        case input of
          Left () -> liftIO (BS.hGetSome h inputBufferSize) >>= returnIfDone
          Right () -> InCmd <$> liftIO (atomically (readTChan cmdChan))
      else
        returnIfDone unconsumed
  where
    returnIfDone bytes = parseBytes bytes >>= maybe (waitForInput mqtt h) return

-- | Parse the given 'ByteString', returning 'Nothing' if more input is needed.
parseBytes :: Monad m => ByteString -> StateT MqttState m (Maybe Input)
parseBytes bytes = do
    parseCC <- gets msParseCC
    case parseCC bytes of
        Fail _unconsumed context err ->
          return $ Just $ InErr $ ParseFailed context err
        Partial cont -> do
          modify $ \s -> s { msParseCC = cont
                           , msUnconsumed = BS.empty }
          return Nothing
        Done unconsumed someMsg -> do
          modify $ \s -> s { msParseCC = parse message
                           , msUnconsumed = unconsumed }
          return $ Just $ InMsg someMsg

handleMessage :: MQTT -> SomeMessage -> StateT MqttState IO ()
handleMessage mqtt (SomeMessage msg) =
    case toSMsgType msg %~ SPUBLISH of
      Proved Refl -> liftIO $ void $ forkMQTT mqtt $ publishHandler mqtt msg
      Disproved _ -> do
        waiting' <- gets msWaiting >>= liftIO . filterM passOnMatching
        modify (\s -> s { msWaiting = waiting' })
  where
    passOnMatching :: AwaitMessage -> IO Bool
    passOnMatching (AwaitMessage (var :: MVar (Message t')) mMsgID')
      | isNothing mMsgID || mMsgID == mMsgID' =
        case toSMsgType msg %~ (sing :: SMsgType t') of
          Proved Refl -> putMVar var msg >> return False
          Disproved _ -> return True
      | otherwise = return True

    mMsgID = getMsgID (body msg)

keepAliveLoop :: MQTT -> MVar () -> IO ()
keepAliveLoop mqtt signal = forever $ do
    let tout = fromJust $ cKeepAlive $ config mqtt
    rslt <- timeout (tout * 10^6) (takeMVar signal)
    case rslt of
      Nothing -> void $ sendAwait mqtt
                  (Message (Header False NoConfirm False) PingReq)
                  SPINGRESP
      Just _ -> return ()

publishHandler :: MQTT -> Message PUBLISH -> IO ()
publishHandler mqtt msg = do
    case (qos (header msg), pubMsgID (body msg)) of
      (Confirm, Just msgid) -> do
          release
          send mqtt $ Message (Header False NoConfirm False) (PubAck msgid)
      (Handshake, Just msgid) -> do
          _ <- sendAwait mqtt
                (Message (Header False NoConfirm False) (PubRec msgid))
                SPUBREL
          release
          send mqtt $ Message (Header False NoConfirm False) (PubComp msgid)
      _ -> release
  where
    release = writeEvent mqtt $ Published msg


-- | Runs the `IO` action in a seperate thread and cancels it if the main MQTT
-- loop exits earlier.
forkMQTT :: MQTT -> IO () -> IO (Async.Async ())
forkMQTT mqtt action = Async.async $ Async.withAsync action $ \_forked ->
    readMVar (mqttAsync mqtt) >>= Async.wait

writeCmd :: MQTT -> Command -> IO ()
writeCmd mqtt = atomically . writeTChan (commands mqtt)

writeEvent :: MQTT -> Event -> IO ()
writeEvent mqtt = atomically . writeTChan (events mqtt)
