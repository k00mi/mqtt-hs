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
>>> TODO
-}
module Network.MQTT
  ( -- * Setup
    run
  , Terminated(..)
  , disconnect
  , Config
  , defaultConfig
  , Commands
  , mkCommands
  -- ** Config accessors
  , cHost
  , cPort
  , cClean
  , cWill
  , cUsername
  , cPassword
  , cKeepAlive
  , cClientID
  , cLogDebug
  , cPublished
  , cCommands
  -- * Subscribing and publishing
  , subscribe
  , unsubscribe
  , publish
  -- * Reexports
  , module Network.MQTT.Types
  ) where

import Control.Applicative ((<$>))
import Control.Concurrent
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.STM
import Control.Exception (bracketOnError, finally)
import Control.Monad (void, forever, filterM)
import Control.Monad.IO.Class (liftIO, MonadIO)
import Control.Monad.Loops (untilJust)
import Control.Monad.State.Strict (evalStateT, gets, modify, StateT)
import Data.Attoparsec.ByteString (IResult(..) , parse)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Foldable (for_)
import Data.Maybe (isNothing, fromJust, fromMaybe)
import Data.Singletons (SingI(..))
import Data.Singletons.Decide
import Data.Text (Text)
import Data.Unique
import Data.Word (Word16)
import Network
import System.IO (Handle, hSetBinaryMode, hLookAhead)
import System.Timeout (timeout)

import Network.MQTT.Types
import Network.MQTT.Parser (message)
import Network.MQTT.Encoding (writeTo)

-----------------------------------------
-- Interface
-----------------------------------------

-- | Reasons for why the connection was terminated.
data Terminated
    = ParseFailed [String] String
    -- ^ at the context in @['String']@ with the given message.
    | ConnectFailed ConnectError
    | UserRequested
    -- ^ 'disconnect' was called
    deriving Show

-- | Commands from the user for the library.
data Command
    = CmdDisconnect
    | CmdSend SomeMessage
    | CmdAwait AwaitMessage
    | CmdStopWaiting AwaitMessage

-- | The communication channel used by 'publish', 'subscribe', etc.
newtype Commands = Cmds { getCmds :: TChan Command }
mkCommands :: IO Commands
mkCommands = Cmds <$> newTChanIO

data AwaitMessage where
    AwaitMessage :: SingI t => MVar (Message t) -> Maybe MsgID -> AwaitMessage

instance Eq AwaitMessage where
  AwaitMessage (var :: MVar (Message t)) mMsgID == AwaitMessage (var' :: MVar (Message t')) mMsgID' =
    case (sing :: SMsgType t) %~ (sing :: SMsgType t') of
      Proved Refl -> mMsgID == mMsgID' && var == var'
      Disproved _ -> False

-- | The various options when establishing a connection.
data Config
    = Config
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
        , cKeepAlive :: Maybe Word16
        -- ^ Time (in seconds) after which a 'PingReq' is sent to the broker if
        -- no regular message was sent. 'Nothing' means no limit.
        , cClientID :: Text
        -- ^ Client ID used by the server to identify clients.
        , cLogDebug :: String -> IO ()
        -- ^ Function for debug-level logging.
        , cResendTimeout :: Int
        -- ^ Time in microseconds after which messages that have not been but
        -- should be acknowledged are retransmitted.
        , cPublished :: TChan (Message PUBLISH)
        -- ^ The channel received 'Publish' messages are written to.
        , cCommands :: Commands
        -- ^ The channel used by 'publish', 'subscribe', etc.
        }

-- | Defaults for 'Config', connects to a server running on
-- localhost.
defaultConfig :: Commands -> TChan (Message PUBLISH) -> Config
defaultConfig commands published = Config
    { cHost             = "localhost"
    , cPort             = 1883
    , cClean            = True
    , cWill             = Nothing
    , cUsername         = Nothing
    , cPassword         = Nothing
    , cKeepAlive        = Nothing
    , cClientID         = "mqtt-haskell"
    , cResendTimeout    = 20 * 10^6 -- 20 seconds
    , cLogDebug         = const $ return ()
    , cCommands         = commands
    , cPublished        = published
    }


-- | Connect to the configured broker, write received 'Publish' messages to the
-- 'cPublished' channel and handle commands from the 'cCommands' channel.
--
-- Exceptions are propagated.
run :: Config -> IO Terminated
run conf = do
    h <- connectTo (cHost conf) (PortNumber $ cPort conf)
    hSetBinaryMode h True
    terminatedVar <- newEmptyTMVarIO
    sendSignal <- newEmptyMVar
    mainLoop conf h (readTMVar terminatedVar) sendSignal
      `finally` atomically (putTMVar terminatedVar ())

-- | Close the connection after sending a 'Disconnect' message.
--
-- See also: 'Will'
disconnect :: Config -> IO ()
disconnect mqtt = writeCmd mqtt CmdDisconnect

-- | Tell the 'mainLoop' to send the given 'Message'.
send :: SingI t => Config -> Message t -> IO ()
send mqtt = writeCmd mqtt . CmdSend . SomeMessage

-- | Tell the 'MQTT' instance to place the next 'Message' of correct
-- 'MsgType' and 'MsgID' (if present) into the 'MVar'.
await :: SingI t => Config -> MVar (Message t) -> Maybe MsgID
            -> IO AwaitMessage
await mqtt var mMsgID = do
    writeCmd mqtt $ CmdAwait awaitMsg
    return awaitMsg
  where
    awaitMsg = AwaitMessage var mMsgID

-- | Stop waiting for the described 'Message'.
stopWaiting :: Config -> AwaitMessage -> IO ()
stopWaiting mqtt = writeCmd mqtt . CmdStopWaiting

-- | Execute the common pattern of sending a message and awaiting
-- a response in a safe, non-racy way. The message message is retransmitted
-- if no response has been received after 'cResendTimeout' microseconds, with
-- exponential backoff for further retransmissions
--
-- An incoming message is considered a response if it is of the
-- requested type and the 'MsgID's match (if present).
sendAwait :: (SingI t, SingI r)
          => Config -> Message t -> SMsgType r -> IO (Message r)
sendAwait mqtt msg _responseS = do
    var <- newEmptyMVar
    let mMsgID = getMsgID (body msg)
    bracketOnError
      (await mqtt var mMsgID)
      (stopWaiting mqtt)
      (\_ ->
        let wait = do
              received <- readMVar var
              if isNothing mMsgID || mMsgID == getMsgID (body received)
                then return received
                else wait
            keepTrying msg' tout = do
              send mqtt msg'
              let retransmit = do
                    cLogDebug mqtt "No response within timeout, retransmitting..."
                    keepTrying (setDup msg') (tout * 2)
              timeout tout wait >>= maybe retransmit return
        in keepTrying msg (cResendTimeout mqtt))

-- | Subscribe to the 'Topic's with the corresponding 'QoS'.
-- Returns the 'QoS' that were granted (lower or equal to the ones requested)
-- in the same order.
--
-- The 'Topic's may contain
-- <http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#appendix-a wildcards>.
subscribe :: Config -> [(Topic, QoS)] -> IO [QoS]
subscribe mqtt topics = do
    msgID <- fromIntegral . hashUnique <$> newUnique
    msg <- sendAwait mqtt
             (Message
               (Header False Confirm False)
               (Subscribe msgID topics))
             SSUBACK
    return $ granted $ body $ msg

-- | Unsubscribe from the given 'Topic's.
unsubscribe :: Config -> [Topic] -> IO ()
unsubscribe mqtt topics = do
    msgID <- fromIntegral . hashUnique <$> newUnique
    void $ sendAwait mqtt
           (Message (Header False Confirm False) (Unsubscribe msgID topics))
           SUNSUBACK

-- | Publish a message to the given 'Topic' at the requested 'QoS' level.
-- The payload can be any sequence of bytes, including none at all.
-- 'True' means the server should retain the message for future subscribers to
-- the topic.
--
-- The 'Topic' must not contain
-- <http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#appendix-a wildcards>.
publish :: Config -> QoS -> Bool -> Topic -> ByteString -> IO ()
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

type WaitTerminate = STM ()
type SendSignal = MVar ()

mainLoop :: Config -> Handle -> WaitTerminate -> SendSignal -> IO Terminated
mainLoop mqtt h waitTerminate sendSignal = do
    void $ forkMQTT waitTerminate $ keepAliveLoop mqtt sendSignal
    evalStateT
      (handshake >>= maybe (liftIO (cLogDebug mqtt "Connected") >> go) return)
      (MqttState (parse message) BS.empty [])
  where
    go = do
      input <- waitForInput mqtt h
      case input of
        InErr err -> liftIO $
          return err
        InMsg someMsg -> do
          liftIO $ cLogDebug mqtt $ "Received " ++ show (toMsgType' someMsg)
          handleMessage mqtt waitTerminate someMsg
          go
        InCmd cmd -> case cmd of
          CmdDisconnect -> liftIO $ do
            doSend msgDisconnect
            return UserRequested
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
        msgConnect = Message
                    (Header False NoConfirm False)
                    (Connect
                      (cClean mqtt)
                      (cWill mqtt)
                      (MqttText $ cClientID mqtt)
                      (MqttText <$> cUsername mqtt)
                      (MqttText <$> cPassword mqtt)
                      (fromMaybe 0 $ cKeepAlive mqtt))

    msgDisconnect = Message (Header False NoConfirm False) Disconnect

    doSend :: (MonadIO io, SingI t) => Message t -> io ()
    doSend msg = liftIO $ do
        cLogDebug mqtt $ "Sending " ++ show (toMsgType msg)
        writeTo h msg
        void $ tryPutMVar sendSignal ()

waitForInput :: Config -> Handle -> StateT MqttState IO Input
waitForInput mqtt h = do
    let cmdChan = getCmds $ cCommands mqtt
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
          Left () -> liftIO (BS.hGetSome h inputBufferSize) >>= parseUntilDone
          Right () -> InCmd <$> liftIO (atomically (readTChan cmdChan))
      else
        parseUntilDone unconsumed
  where
    parseUntilDone bytes = parseBytes bytes >>= maybe (waitForInput mqtt h) return

-- | Parse the given 'ByteString' and update the current 'MqttState'.
--
-- Returns 'Nothing' if more input is needed.
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

handleMessage :: Config -> WaitTerminate -> SomeMessage -> StateT MqttState IO ()
handleMessage mqtt waitTerminate (SomeMessage msg) =
    case toSMsgType msg %~ SPUBLISH of
      Proved Refl -> liftIO $ void $ forkMQTT waitTerminate $ publishHandler mqtt msg
      Disproved _ -> do
        waiting' <- gets msWaiting >>= liftIO . filterM giveToWaiting
        modify (\s -> s { msWaiting = waiting' })
  where
    giveToWaiting :: AwaitMessage -> IO Bool
    giveToWaiting (AwaitMessage (var :: MVar (Message t')) mMsgID')
      | isNothing mMsgID || mMsgID == mMsgID' =
        case toSMsgType msg %~ (sing :: SMsgType t') of
          Proved Refl -> putMVar var msg >> return False
          Disproved _ -> return True
      | otherwise = return True

    mMsgID = getMsgID (body msg)

keepAliveLoop :: Config -> SendSignal -> IO ()
keepAliveLoop mqtt signal = for_ (cKeepAlive mqtt) $ \tout -> forever $ do
    rslt <- timeout (fromIntegral tout * 10^6) (takeMVar signal)
    case rslt of
      Nothing -> void $ sendAwait mqtt
                  (Message (Header False NoConfirm False) PingReq)
                  SPINGRESP
      Just _ -> return ()

publishHandler :: Config -> Message PUBLISH -> IO ()
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
    release = writeTChanIO (cPublished mqtt) msg


-- | Runs the 'IO' action in a seperate thread and cancels it if the 'mainLoop'
-- exits earlier.
forkMQTT :: WaitTerminate -> IO () -> IO (Async.Async ())
forkMQTT waitTerminate action = Async.async $ Async.withAsync action $ \forked ->
    atomically $ waitTerminate `orElse` Async.waitSTM forked

writeTChanIO :: TChan a -> a -> IO ()
writeTChanIO chan = atomically . writeTChan chan

writeCmd :: Config -> Command -> IO ()
writeCmd mqtt = writeTChanIO (getCmds $ cCommands mqtt)
