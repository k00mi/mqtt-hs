{-# Language ConstraintKinds, FlexibleContexts #-}
{-|
Module: MQTT
Copyright: Lukas Braun 2014
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

Wrappers for the functions in @Network.MQTT@ that carry the 'MQTT' parameter
in a 'MonadReader'.

For documentation see the corresponding functions in @Network.MQTT@.

A note on the functions using 'MonadBaseControl': Any changes to the monadic
state are discarded, the functions are only called for their side-effects.
-}
module Network.MQTT.Monadic
  ( HasMQTT(..)
  , MonadMQTT
  -- * Creating connections
  , connect
  , disconnect
  , reconnect
  , onReconnect
  , resubscribe
  -- * Connection settings
  , MQTTConfig
  , MQTT.defaultConfig
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

import Control.Monad.IO.Class
import Control.Monad.Reader
import Control.Monad.Trans.Control
import Data.ByteString (ByteString)
import Data.Singletons (SingI(..))
import Data.Unique

import Network.MQTT (MQTT, MQTTConfig(..))
import qualified Network.MQTT as MQTT
import Network.MQTT.Types

-- | A class for things that contain a 'MQTT' connection.
class HasMQTT r where
    getMQTT :: r -> MQTT

instance HasMQTT MQTT where
    getMQTT = id

-- | The constraint most of the functions in this module have in common.
type MonadMQTT r m = (HasMQTT r, MonadReader r m, MonadIO m)

connect :: MonadIO m => MQTT.MQTTConfig -> m (Maybe MQTT)
connect = liftIO . MQTT.connect

disconnect :: MonadMQTT r m => m ()
disconnect = asks getMQTT >>= liftIO . MQTT.disconnect

reconnect :: MonadMQTT r m => Int -> m ()
reconnect n = asks getMQTT >>= liftIO . flip MQTT.reconnect n

onReconnect :: (MonadBaseControl IO m, MonadMQTT r m) => m () -> m ()
onReconnect action = do
    mqtt <- asks getMQTT
    liftBaseDiscard (MQTT.onReconnect mqtt) action

resubscribe :: MonadMQTT r m => m (Maybe [QoS])
resubscribe = asks getMQTT >>= liftIO . MQTT.resubscribe

subscribe :: (MonadBaseControl IO m, MonadMQTT r m)
          => QoS -> Topic -> (Topic -> ByteString -> m ()) -> m QoS
subscribe qos topic callback = do
    mqtt <- asks getMQTT
    liftBaseWith $ \runInBase ->
      MQTT.subscribe mqtt qos topic (\t bs -> void $ runInBase $ callback t bs)

unsubscribe :: MonadMQTT r m => Topic -> m ()
unsubscribe topic = asks getMQTT >>= liftIO . flip MQTT.unsubscribe topic

publish :: MonadMQTT r m
        => QoS -> Bool -> Topic -> ByteString -> m ()
publish qos retain topic payload = do
    mqtt <- asks getMQTT
    liftIO $ MQTT.publish mqtt qos retain topic payload

send :: MonadMQTT r m
     => Message t -> m ()
send msg = asks getMQTT >>= liftIO . flip MQTT.send msg

addHandler :: (MonadBaseControl IO m, MonadMQTT r m, SingI t)
           => (Message t -> m ()) -> m Unique
addHandler callback = do
    mqtt <- asks getMQTT
    liftBaseWith $ \runInBase ->
      MQTT.addHandler mqtt (void . runInBase . callback)

removeHandler :: MonadMQTT r m
              => Unique -> m ()
removeHandler mhID = asks getMQTT >>= liftIO . flip MQTT.removeHandler mhID

awaitMsg :: (MonadMQTT r m, SingI t)
         => SMsgType t -> Maybe MsgID -> m (Message t)
awaitMsg mtype mID = do
    mqtt <- asks getMQTT
    liftIO $ MQTT.awaitMsg mqtt mtype mID

awaitMsg' :: (MonadMQTT r m, SingI t)
          => Maybe MsgID -> m (Message t)
awaitMsg' = awaitMsg sing
