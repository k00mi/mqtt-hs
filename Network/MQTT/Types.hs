{-# Language GeneralizedNewtypeDeriving,
             PatternSynonyms,
             OverloadedStrings,
             DataKinds,
             KindSignatures,
             GADTs,
             TypeFamilies,
             ScopedTypeVariables,
             RankNTypes,
             TemplateHaskell
             #-}
{-|
Module: MQTT.Types
Copyright: Lukas Braun 2014
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

Types representing MQTT messages.
-}
module Network.MQTT.Types
  ( -- * Messages
    Message(..)
  , SomeMessage(..)
  , MqttHeader(..)
  -- * Message bodies
  , MessageBody(..)
  , Connect(..)
  , ConnAck(..)
  , Publish(..)
  , Subscribe(..)
  , SubAck(..)
  , Unsubscribe(..)
  , SimpleMsg(..)
  -- * Miscellaneous
  , Will(..)
  , QoS(..)
  , MsgID
  , getMsgID
  , Topic
  , matches
  , fromTopic
  , toTopic
  , getLevels
  , fromLevels
  , MqttText(..)
  -- * Message types
  , MsgType(..)
  , toMsgType
  , toMsgType'
  -- ** Singletons
  -- | Singletons are used to build a bridge between the type and value level.
  -- See the @singletons@ package for more information.
  --
  -- You do not have to use or understand these in order to use this
  -- library, they are mostly used internally to get better guarantees
  -- about the flow of 'Message's.
  , toSMsgType
  , SMsgType
  , withSomeSingI
  , Sing( SCONNECT
        , SCONNACK
        , SPUBLISH
        , SPUBACK
        , SPUBREC
        , SPUBREL
        , SPUBCOMP
        , SSUBSCRIBE
        , SSUBACK
        , SUNSUBSCRIBE
        , SUNSUBACK
        , SPINGREQ
        , SPINGRESP
        , SDISCONNECT)
  ) where

import Data.ByteString (ByteString)
import Data.Singletons.TH
import Data.String (IsString(..))
import Data.Text (Text)
import qualified Data.Text as T
import Data.Word

-- | A MQTT message, indexed by the type of the message ('MsgType').
data Message (t :: MsgType)
    = Message
        { header :: MqttHeader
        , body :: MessageBody t
        }

-- | Any message, hiding the index.
data SomeMessage where
    SomeMessage :: SingI t => Message t -> SomeMessage

-- | Fixed header required in every message.
data MqttHeader
    = Header
        { -- msgType :: MsgType  -- ^ Type of the message
          dup :: Bool         -- ^ Has this message been sent before?
        , qos :: QoS          -- ^ Quality of Service-level
        , retain :: Bool      -- ^ Should the broker retain the message for
                              -- future subscribers?
        }
    deriving (Eq, Ord, Show)

-- | The body of a MQTT message, indexed by the type of the message ('MsgType').
data MessageBody (t :: MsgType) where
    MConnect      :: Connect      -> MessageBody CONNECT
    MConnAck      :: ConnAck      -> MessageBody CONNACK
    MPublish      :: Publish      -> MessageBody PUBLISH
    MPubAck       :: SimpleMsg    -> MessageBody PUBACK
    MPubRec       :: SimpleMsg    -> MessageBody PUBREC
    MPubRel       :: SimpleMsg    -> MessageBody PUBREL
    MPubComp      :: SimpleMsg    -> MessageBody PUBCOMP
    MSubscribe    :: Subscribe    -> MessageBody SUBSCRIBE
    MSubAck       :: SubAck       -> MessageBody SUBACK
    MUnsubscribe  :: Unsubscribe  -> MessageBody UNSUBSCRIBE
    MUnsubAck     :: SimpleMsg    -> MessageBody UNSUBACK
    MPingReq      ::                 MessageBody PINGREQ
    MPingResp     ::                 MessageBody PINGRESP
    MDisconnect   ::                 MessageBody DISCONNECT

-- | The fields of a CONNECT message.
data Connect
    = Connect
        { cleanSession :: Bool
        -- ^ Should the server forget subscriptions and other state on
        -- disconnects?
        , will :: Maybe Will
        -- ^ Optional 'Will' message.
        , clientID :: MqttText
        -- ^ Client ID used by the server to identify clients.
        , username :: Maybe MqttText
        -- ^ Optional username used for authentication.
        , password :: Maybe MqttText
        -- ^ Optional password used for authentication.
        , keepAlive :: Word16
        -- ^ Maximum interval (in seconds) in which a message must be sent.
        -- 0 means no limit.
        } deriving (Show, Eq)

-- | The response to a CONNECT. Anything other than 0 means the broker
-- refused the connection
-- (<http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#connack details>).
newtype ConnAck = ConnAck { returnCode :: Word8 }
          deriving (Show, Eq)

-- | The fields of a PUBLISH message.
data Publish
    = Publish
        { topic :: Topic
        -- ^ The 'Topic' to which the message should be published.
        , pubMsgID :: Maybe MsgID
        -- ^ 'MsgID' of the message if 'QoS' > 'NoConfirm'.
        , payload :: ByteString
        -- ^ The content that will be published.
        } deriving (Show, Eq)

-- | The fields of a SUBSCRIBE message.
data Subscribe
    = Subscribe
        { subscribeMsgID :: MsgID
        , subTopics :: [(Topic, QoS)]
        -- ^ The 'Topic's and corresponding requested 'QoS'.
        } deriving (Show, Eq)

-- | The fields of a SUBACK message.
data SubAck
    = SubAck
        { subAckMsgID :: MsgID
        , granted :: [QoS]
        -- ^ The 'QoS' granted for each 'Topic' in the order they were sent
        -- in the SUBSCRIBE.
        } deriving (Show, Eq)

-- | The fields of a UNSUBSCRIBE message.
data Unsubscribe
    = Unsubscribe
        { unsubMsgID :: MsgID
        , unsubTopics :: [Topic]
        -- ^ The 'Topic's from which the client should be unsubscribed.
        } deriving (Show, Eq)

-- | Any message body that consists only of a 'MsgID'.
newtype SimpleMsg = SimpleMsg { msgID :: MsgID }
          deriving (Show, Eq)

-- | The different levels of QoS
data QoS
    = NoConfirm -- ^ Fire and forget, message will be published at most once.
    | Confirm   -- ^ Acknowledged delivery, message will be published at least once.
    | Handshake -- ^ Assured delivery, message will be published exactly once.
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

type MsgID = Word16

-- | Get the message ID of any message, if it exists.
getMsgID :: MessageBody t -> Maybe MsgID
getMsgID (MConnect _)         = Nothing
getMsgID (MConnAck _)         = Nothing
getMsgID (MPublish pub)       = pubMsgID pub
getMsgID (MPubAck simple)     = Just (msgID simple)
getMsgID (MPubRec simple)     = Just (msgID simple)
getMsgID (MPubRel simple)     = Just (msgID simple)
getMsgID (MPubComp simple)    = Just (msgID simple)
getMsgID (MSubscribe sub)     = Just (subscribeMsgID sub)
getMsgID (MSubAck subA)       = Just (subAckMsgID subA)
getMsgID (MUnsubscribe unsub) = Just (unsubMsgID unsub)
getMsgID (MUnsubAck simple)   = Just (msgID simple)
getMsgID MPingReq             = Nothing
getMsgID MPingResp            = Nothing
getMsgID MDisconnect          = Nothing

-- | A topic is a "hierarchical name space that defines a taxonomy of
-- information sources for which subscribers can register an interest."
--
-- See
-- <http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#appendix-a here>
-- for more information on topics.
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
        go [] (l:_)          = l == "#"
        go (l:_) []          = l == "#"
        go (l1:ls1) (l2:ls2) = l1 == "#" || l2 == "#"
                                || ((l1 == "+" || l2 == "+" || l1 == l2)
                                    && go ls1 ls2)

toTopic :: MqttText -> Topic
toTopic (MqttText txt) = Topic (T.split (== '/') txt) txt

fromTopic :: Topic -> MqttText
fromTopic = MqttText . orig

-- | Split a topic into its individual levels.
getLevels :: Topic -> [Text]
getLevels = levels

-- | Create a 'Topic' from its individual levels.
fromLevels :: [Text] -> Topic
fromLevels ls = Topic ls (T.intercalate "/" ls)

instance IsString Topic where
    fromString str = let txt = T.pack str in
      Topic (T.split (== '/') txt) txt

-- | The various types of messages.
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

genSingletons [''MsgType]
singDecideInstance ''MsgType

-- | Determine the 'MsgType' of a 'Message'.
toMsgType :: SingI t => Message t -> MsgType
toMsgType = fromSing . toSMsgType

-- | Determine the 'MsgType' of a 'SomeMessage'.
toMsgType' :: SomeMessage -> MsgType
toMsgType' (SomeMessage msg) = toMsgType msg

-- | Determine the singleton 'SMsgType' of a 'Message'.
toSMsgType :: SingI t => Message t -> SMsgType t
toSMsgType _ = sing

-- | Helper to generate both an implicit and explicit singleton.
withSomeSingI :: MsgType -> (forall t. SingI t => SMsgType t -> r) -> r
withSomeSingI t f = withSomeSing t $ \s -> withSingI s $ f s
