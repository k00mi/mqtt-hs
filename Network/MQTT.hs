{-# Language OverloadedStrings,
             DataKinds,
             ScopedTypeVariables,
             GADTs,
             DeriveDataTypeable #-}
{-|
Module: MQTT
Copyright: Lukas Braun 2014-2016
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

An MQTT client library.
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
import Control.Concurrent.STM
import Control.Exception (finally)
import Control.Monad (void)
import Data.ByteString (ByteString)
import Data.Maybe (fromJust)
import Data.Unique
import Network
import System.IO (hSetBinaryMode)

import Network.MQTT.Internal
import Network.MQTT.Types


-- | Defaults for 'Config', connects to a server running on
-- localhost.
defaultConfig :: Commands -> TChan (Message 'PUBLISH) -> Config
defaultConfig commands published = Config
    { cHost             = "localhost"
    , cPort             = 1883
    , cClean            = True
    , cWill             = Nothing
    , cUsername         = Nothing
    , cPassword         = Nothing
    , cKeepAlive        = Nothing
    , cClientID         = "mqtt-haskell"
    , cResendTimeout    = secToMicro 20
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

