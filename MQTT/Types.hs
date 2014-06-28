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
module MQTT.Types where

import Data.ByteString (ByteString)
import Data.Monoid
import Data.String
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word
import Data.String (IsString)


data Message
    = Message
        { header :: MqttHeader
        , varHeader :: Maybe VarHeader
        , payload :: Maybe Payload
        }
    deriving (Eq, Show)


---------------------------------
-- * Fixed Header
---------------------------------

-- | Fixed header required in every message
data MqttHeader
    = Header
        { msgType :: MsgType  -- ^ Type of the message
        , dup :: Bool         -- ^ Has this message been sent before?
        , qos :: QoS          -- ^ Quality of Service-level
        , retain :: Bool      -- ^ Should the broker retain the message for
                              -- future subscribers?
        }
    deriving (Eq, Ord, Show)

-- | The various types of commands
data MsgType
    = CONNECT
    | CONNACK
    | PUBLISH
    | PUBACK
    | PUBREC
    | PUBREL
    | PUBCOMP
    | SUBSCRIBE
    | SUBACK
    | UNSUBSCRIBE
    | UNSUBACK
    | PINGREQ
    | PINGRESP
    | DISCONNECT
    deriving (Eq, Enum, Ord, Show)

-- | The different levels of QoS
data QoS
    = NoConfirm -- ^ Fire and forget
    | Confirm   -- ^ Acknowledged delivery (repeat until ack)
    | Handshake -- ^ Assured delivery (four-step handshake)
    deriving (Eq, Ord, Enum, Show)


---------------------------------
-- * Variable Headers
---------------------------------

data VarHeader
    = VHConnect ConnectHeader
    | VHConnAck Word8
    | VHPublish PublishHeader
    | VHOther MsgID
    deriving (Eq, Show)

type MsgID = Word16

getMsgID :: VarHeader -> Maybe MsgID
getMsgID (VHPublish ph) = messageID ph
getMsgID (VHOther id)   = Just id
getMsgID _              = Nothing

data ConnectHeader
    = ConnectHeader
        { protocolName :: MqttText  -- ^ Should be "MQIsdp"
        , protocolVersion :: Word8  -- ^ Should be 3
        , cleanSession :: Bool      -- ^ Should the server reset settings
                                    -- after reconnects?
        , will :: Maybe (QoS, Bool) -- ^ Will message QoS and retain flag.
                                    -- 'Nothing' means no Will message.
        , usernameFlag :: Bool
        , passwordFlag :: Bool
        , keepAlive :: Word16       -- ^ Interval in seconds in which the
                                    -- client must send a message.
        }
    deriving (Eq, Show)

data PublishHeader
    = PublishHeader
        { topic :: Topic
        , messageID :: Maybe MsgID
        }
    deriving (Eq, Show)


---------------------------------
-- * Payload
---------------------------------

data Payload
    = PLConnect ConnectPL
    | PLPublish ByteString
    | PLSubscribe [(Topic, QoS)]
    | PLSubAck [QoS]
    | PLUnsubscribe [Topic]
    deriving (Eq, Show)

data ConnectPL
    = ConnectPL
        { clientID :: MqttText
        , willTopic :: Maybe MqttText
        , willMsg :: Maybe MqttText
        , username :: Maybe MqttText
        , password :: Maybe MqttText
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


---------------------------------
-- * Pattern Synonyms
---------------------------------

pattern ConnAck code <-
    Message
      (Header CONNACK _ _ _)
      (Just (VHConnAck code))
      Nothing

pattern Publish topic body <-
    Message
      (Header PUBLISH _ _ _)
      (Just (VHPublish (PublishHeader topic _)))
      (Just (PLPublish body))

pattern PubConfirm msgid <-
    Message
      (Header PUBLISH _ Confirm _)
      (Just (VHPublish (PublishHeader _ (Just msgid))))
      _

pattern PubHandshake msgid <-
    Message
      (Header PUBLISH _ Handshake _)
      (Just (VHPublish (PublishHeader _ (Just msgid))))
      _
