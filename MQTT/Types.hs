{-# Language GeneralizedNewtypeDeriving,
             PatternSynonyms,
             OverloadedStrings
             #-}
{-|
Module: MQTT.Types
Copyright: Lukas Braun 2014
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

Types representing MQTT messages.
-}
module MQTT.Types
  ( Message(..)
  , MqttHeader(..)
  , MessageBody(..)
  , Connect(..)
  , ConnAck(..)
  , Publish(..)
  , Subscribe(..)
  , SubAck(..)
  , Unsubscribe(..)
  , SimpleMsg(..)
  , Will(..)
  , QoS(..)
  , MsgID
  , Topic
  , fromTopic
  , toTopic
  , matches
  , MqttText(..)
  ) where

import Data.ByteString (ByteString)
import Data.String (IsString(..))
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word


data Message
    = Message
        { header :: MqttHeader
        , body :: MessageBody
        }
    deriving (Eq, Show)

-- | Fixed header required in every message
data MqttHeader
    = Header
        { -- msgType :: MsgType  -- ^ Type of the message
          dup :: Bool         -- ^ Has this message been sent before?
        , qos :: QoS          -- ^ Quality of Service-level
        , retain :: Bool      -- ^ Should the broker retain the message for
                              -- future subscribers?
        }
    deriving (Eq, Ord, Show)

data MessageBody
    = MConnect Connect
    | MConnAck ConnAck
    | MPublish Publish
    | MPubAck SimpleMsg
    | MPubRec SimpleMsg
    | MPubRel SimpleMsg
    | MPubComp SimpleMsg
    | MSubscribe Subscribe
    | MSubAck SubAck
    | MUnsubscribe Unsubscribe
    | MUnsubAck SimpleMsg
    | MPingReq
    | MPingResp
    | MDisconnect
    deriving (Show, Eq)

data Connect
    = Connect
        { cleanSession :: Bool      -- ^ Should the server reset settings
        , will :: Maybe Will
        , clientID :: MqttText
        , username :: Maybe MqttText
        , password :: Maybe MqttText
        , keepAlive :: Word16
        } deriving (Show, Eq)

newtype ConnAck = ConnAck { returnCode :: Word8 }
          deriving (Show, Eq)

data Publish
    = Publish
        { topic :: Topic
        , pubMsgID :: Maybe MsgID
        , payload :: ByteString
        } deriving (Show, Eq)

data Subscribe
    = Subscribe
        { subscribeMsgID :: MsgID
        , subTopics :: [(Topic, QoS)]
        } deriving (Show, Eq)

data SubAck
    = SubAck
        { subAckMsgID :: MsgID
        , granted :: [QoS]
        } deriving (Show, Eq)

data Unsubscribe
    = Unsubscribe
        { unsubMsgID :: MsgID
        , unsubTopics :: [Topic]
        } deriving (Show, Eq)

newtype SimpleMsg = SimpleMsg { msgID :: MsgID }
          deriving (Show, Eq)

-- | The different levels of QoS
data QoS
    = NoConfirm -- ^ Fire and forget
    | Confirm   -- ^ Acknowledged delivery (repeat until ack)
    | Handshake -- ^ Assured delivery (four-step handshake)
    deriving (Eq, Ord, Enum, Show)

-- | A Will message is published by the broker if a client disconnects
-- without sending a DISCONNECT.
data Will
    = Will
        { wRetain :: Bool
        , wQoS :: QoS
        , wTopic :: Topic
        , wMsg :: MqttText
        }
    deriving (Eq, Show)

-- | MQTT uses length-prefixed UTF-8 as text encoding.
newtype MqttText = MqttText { text :: Text }
    deriving (Eq, Show, IsString)

-- | A topic is a "hierarchical name space that defines a taxonomy of
-- information sources for which subscribers can register an interest."
data Topic = Topic { levels :: [Text], orig :: Text }
-- levels and orig should always refer to the same topic, this way no text
-- has to be copied when converting from/to text

type MsgID = Word16

instance Show Topic where
    show (Topic _ t) = show t

instance Eq Topic where
    Topic _ t1 == Topic _ t2 = t1 == t2

-- | Check if one of the 'Topic's matches the other.
matches :: Topic -> Topic -> Bool
matches (Topic t1 _) (Topic t2 _) = go t1 t2
      where
        go [] []             = True
        go [] (l:ls)         = l == "#"
        go (l:ls) []         = l == "#"
        go (l1:ls1) (l2:ls2) = l1 == "#" || l2 == "#"
                                || ((l1 == "+" || l2 == "+" || l1 == l2)
                                    && go ls1 ls2)

toTopic :: MqttText -> Topic
toTopic (MqttText txt) = Topic (T.split (== '/') txt) txt

fromTopic :: Topic -> MqttText
fromTopic = MqttText . orig

instance IsString Topic where
    fromString str = let txt = T.pack str in
      Topic (T.split (== '/') txt) txt
