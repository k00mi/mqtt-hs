{-# Language ScopedTypeVariables,
             DataKinds,
             GADTs #-}
{-|
Module: Network.MQTT.Internal
Copyright: Lukas Braun 2014-2016
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

MQTT Internals.

Use with care and expected changes.
-}
module Network.MQTT.Internal
  (
  -- * User interaction
    Terminated(..)
  , Command(..)
  , Commands(..)
  , mkCommands
  , AwaitMessage(..)
  , Config(..)
  , send
  , await
  , stopWaiting
  , sendAwait
  , writeCmd
  -- * Main loop
  , mainLoop
  , WaitTerminate
  , SendSignal
  , MqttState(..)
  , ParseCC
  , Input(..)
  , waitForInput
  , parseBytes
  , handleMessage
  , publishHandler
  -- * Misc
  , secToMicro
  ) where

import Control.Applicative ((<$>))
import Control.Concurrent
import qualified Control.Concurrent.Async as Async
import Control.Concurrent.STM
import Control.Exception (bracketOnError)
import Control.Monad (void, forever, filterM)
import Control.Monad.IO.Class (liftIO, MonadIO)
import Control.Monad.Loops (untilJust)
import Control.Monad.State.Strict (evalStateT, gets, modify, StateT)
import Data.Attoparsec.ByteString (IResult(..) , parse)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import Data.Foldable (for_)
import Data.Maybe (isNothing, fromMaybe)
import Data.Singletons (SingI(..))
import Data.Singletons.Decide
import Data.Text (Text)
import Data.Word (Word16)
import Network
import System.IO (Handle, hLookAhead)
import System.Timeout (timeout)

import Network.MQTT.Types
import Network.MQTT.Parser (message)
import Network.MQTT.Encoding (writeTo)

-----------------------------------------
-- User interaction
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
        -- ^ Time in seconds after which messages that have not been but
        -- should be acknowledged are retransmitted.
        , cPublished :: TChan (Message 'PUBLISH)
        -- ^ The channel received 'Publish' messages are written to.
        , cCommands :: Commands
        -- ^ The channel used by 'publish', 'subscribe', etc.
        , cInputBufferSize :: Int
        -- ^ Maximum number of bytes read from the network at once.
        }

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
        in keepTrying msg initialTout)
  where
    initialTout = secToMicro $ cResendTimeout mqtt


-----------------------------------------
-- Main loop
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
        input <- untilJust (getSome mqtt h >>= parseBytes)
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
          Left () -> getSome mqtt h >>= parseUntilDone
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
    rslt <- timeout (secToMicro (fromIntegral tout)) (takeMVar signal)
    case rslt of
      Nothing -> void $ sendAwait mqtt
                  (Message (Header False NoConfirm False) PingReq)
                  SPINGRESP
      Just _ -> return ()

publishHandler :: Config -> Message 'PUBLISH -> IO ()
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

getSome :: MonadIO m => Config -> Handle -> m ByteString
getSome mqtt h = liftIO (BS.hGetSome h (cInputBufferSize mqtt))

-- | Runs the 'IO' action in a seperate thread and cancels it if the 'mainLoop'
-- exits earlier.
forkMQTT :: WaitTerminate -> IO () -> IO (Async.Async ())
forkMQTT waitTerminate action = Async.async $ Async.withAsync action $ \forked ->
    atomically $ waitTerminate `orElse` Async.waitSTM forked

writeTChanIO :: TChan a -> a -> IO ()
writeTChanIO chan = atomically . writeTChan chan

writeCmd :: Config -> Command -> IO ()
writeCmd mqtt = writeTChanIO (getCmds $ cCommands mqtt)

secToMicro :: Int -> Int
secToMicro m = m * 10 ^ (6 :: Int)
