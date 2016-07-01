{-# Language DataKinds, OverloadedStrings, RankNTypes #-}

-- Similar to the `subscribe.hs` example, but creates a Producer for each of
-- the subscribed Topics.
-- The interesting part is the use of `cloneTChan`, which allows us to easily
-- create multiple Producers from the same channel.

module MQTTConduit where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad (when, forever)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Data.Conduit ((=$=), ($$))
import qualified Data.Conduit as C
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)

import qualified Network.MQTT as MQTT

t1, t2 :: MQTT.Topic
t1 = "topic1"
t2 = "topic2/#"


-- first we need a little helper to use a TChan as source
sourceTChan :: MonadIO m
            => TChan a -> C.Producer m a
sourceTChan chan = forever $ liftIO (atomically (readTChan chan)) >>= C.yield


-- A Conduit that only yields messages that were published on the argument topic
filterTopic :: Monad m
            => MQTT.Topic
            -> C.Conduit (MQTT.Message MQTT.PUBLISH) m (MQTT.Message MQTT.PUBLISH)
filterTopic t = C.awaitForever $ \msg ->
    when (t `MQTT.matches` MQTT.topic (MQTT.body msg)) $
      C.yield msg


-- consumers for messages on the topics we are interested in
handleT1 :: C.Consumer (MQTT.Message MQTT.PUBLISH) IO ()
handleT1 = C.awaitForever $ \msg ->
  lift $ putStrLn $ "topic1: " ++ show (MQTT.payload (MQTT.body msg))

handleT2 :: C.Consumer (MQTT.Message MQTT.PUBLISH) IO ()
handleT2 = C.awaitForever $ \msg ->
  let b = MQTT.body msg
  in lift $ putStrLn $ show (MQTT.topic b) ++ ": " ++ show (MQTT.payload b)


main :: IO ()
main = do
  cmds <- MQTT.mkCommands
  -- create one channel per conduit, each one receiving all the messages
  pubChan <- newTChanIO
  pubChan' <- atomically $ cloneTChan pubChan
  let conf = MQTT.defaultConfig cmds pubChan

  _ <- forkIO $ sourceTChan pubChan =$= filterTopic t1 $$ handleT1
  _ <- forkIO $ sourceTChan pubChan' =$= filterTopic t2 $$ handleT2

  _ <- forkIO $ do
    qosGranted <- MQTT.subscribe conf [(t1, MQTT.Handshake), (t2, MQTT.Handshake)]
    case qosGranted of
      [MQTT.Handshake, MQTT.Handshake] -> return ()
      _ -> do
        hPutStrLn stderr $ "Wanted QoS Handshake, got " ++ show qosGranted
        exitFailure

  -- this will throw IOExceptions
  terminated <- MQTT.run conf
  print terminated
