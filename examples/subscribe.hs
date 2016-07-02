{-# Language DataKinds, OverloadedStrings #-}

-- Shows how to connect to a broker, subscribe to a bunch of topics and inspect
-- to which of those a message was published to.
-- Doesn't publish any messages itself, use mosquitto_pub or something else for
-- that.

module Subscribe where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad (unless, forever)
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)

import qualified Network.MQTT as MQTT

t1, t2 :: MQTT.Topic
t1 = "topic1"
t2 = "topic2/#"


handleMsg :: MQTT.Message MQTT.PUBLISH -> IO ()
handleMsg msg =
    -- sometimes it's useful to ignore retained messages
    unless (MQTT.retain $ MQTT.header msg) $ do
      let t = MQTT.topic $ MQTT.body msg
          p = MQTT.payload $ MQTT.body msg
      case MQTT.getLevels t of
        ["topic1"] -> putStr "topic1: " >> print p
        ["topic2", "foo"] -> putStr "foo: " >> print p
        "topic2" : bar   -> print bar
        _unexpected -> putStrLn $ "unexpected message on '" ++ show t ++ "': " ++ show p

main :: IO ()
main = do
  cmds <- MQTT.mkCommands
  pubChan <- newTChanIO
  let conf = (MQTT.defaultConfig cmds pubChan)
              { MQTT.cUsername = Just "mqtt-hs"
              , MQTT.cPassword = Just "secret"
              }

  _ <- forkIO $ do
    qosGranted <- MQTT.subscribe conf [(t1, MQTT.Handshake), (t2, MQTT.Handshake)]
    case qosGranted of
      [MQTT.Handshake, MQTT.Handshake] -> forever $ atomically (readTChan pubChan) >>= handleMsg
      _ -> do
        hPutStrLn stderr $ "Wanted QoS Handshake, got " ++ show qosGranted
        exitFailure

  -- this will throw IOExceptions
  terminated <- MQTT.run conf
  print terminated
