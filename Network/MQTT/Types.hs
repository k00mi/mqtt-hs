{-# Language GeneralizedNewtypeDeriving,
             DeriveDataTypeable,
             OverloadedStrings,
             DataKinds,
             KindSignatures,
             GADTs,
             TypeFamilies,
             ScopedTypeVariables,
             RankNTypes,
             TemplateHaskell,
             EmptyCase
             #-}

-- without -O0 GHC 7.6.3 loops while building, probably related to
-- https://git.haskell.org/ghc.git/commitdiff/c1edbdfd9148ad9f74bfe41e76c524f3e775aaaa
--
-- -fno-warn-unused-binds is used because the Singletons TH magic generates a
-- lot of unused binds and GHC has no way to disable warnings locally
{-# OPTIONS_GHC -O0 -fno-warn-unused-binds #-}

{-|
Module: MQTT.Types
Copyright: Lukas Braun 2014-2016
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

Types representing MQTT messages.
-}
module Network.MQTT.Types
  ( -- * Messages
    Message(..)
  , SomeMessage(..)
  , MqttHeader(..)
  , setDup
  -- * Message body
  , MessageBody(..)
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
  , ConnectError(..)
  , toConnectError
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

import Control.Exception (Exception)
import Data.ByteString (ByteString)
import Data.Singletons
import Data.Singletons.TH
import Data.String (IsString(..))
import Data.Text (Text)
import qualified Data.Text as T
import Data.Typeable (Typeable)
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
        { dup :: Bool         -- ^ Has this message been sent before?
        , qos :: QoS          -- ^ Quality of Service-level
        , retain :: Bool      -- ^ Should the broker retain the message for
                              -- future subscribers?
        }
    deriving (Eq, Ord, Show)

-- | Set the 'dup' flag to 'True'.
setDup :: Message t -> Message t
setDup (Message h b) = Message h { dup = True } b

-- | The body of a MQTT message, indexed by the type of the message ('MsgType').
data MessageBody (t :: MsgType) where
    Connect     :: { cleanSession :: Bool
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
                   -- ^ Time (in seconds) after which a 'PingReq' is sent to the broker if
                   -- no regular message was sent. 0 means no limit.
                   }                              -> MessageBody 'CONNECT
    ConnAck     :: { returnCode :: Word8 }        -> MessageBody 'CONNACK
    Publish     :: { topic :: Topic
                   -- ^ The 'Topic' to which the message should be published.
                   , pubMsgID :: Maybe MsgID
                   -- ^ 'MsgID' of the message if 'QoS' > 'NoConfirm'.
                   , payload :: ByteString
                   -- ^ The content that will be published.
                   }                              -> MessageBody 'PUBLISH
    PubAck      :: { pubAckMsgID :: MsgID }       -> MessageBody 'PUBACK
    PubRec      :: { pubRecMsgID :: MsgID }       -> MessageBody 'PUBREC
    PubRel      :: { pubRelMsgID :: MsgID }       -> MessageBody 'PUBREL
    PubComp     :: { pubCompMsgID :: MsgID }      -> MessageBody 'PUBCOMP
    Subscribe   :: { subscribeMsgID :: MsgID
                   , subTopics :: [(Topic, QoS)]
                   -- ^ The 'Topic's and corresponding requested 'QoS'.
                   }                              -> MessageBody 'SUBSCRIBE
    SubAck      :: { subAckMsgID :: MsgID
                   , granted :: [QoS]
                   -- ^ The 'QoS' granted for each 'Topic' in the order they were sent
                   -- in the SUBSCRIBE.
                   }                              -> MessageBody 'SUBACK
    Unsubscribe :: { unsubMsgID :: MsgID
                   , unsubTopics :: [Topic]
                   -- ^ The 'Topic's from which the client should be unsubscribed.
                   }                              -> MessageBody 'UNSUBSCRIBE
    UnsubAck    :: { unsubAckMsgID :: MsgID }     -> MessageBody 'UNSUBACK
    PingReq     ::                                   MessageBody 'PINGREQ
    PingResp    ::                                   MessageBody 'PINGRESP
    Disconnect  ::                                   MessageBody 'DISCONNECT


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
getMsgID (Connect{})           = Nothing
getMsgID (ConnAck{})           = Nothing
getMsgID (Publish _ mMsgid _)  = mMsgid
getMsgID (PubAck msgid)        = Just msgid
getMsgID (PubRec msgid)        = Just msgid
getMsgID (PubRel msgid)        = Just msgid
getMsgID (PubComp msgid)       = Just msgid
getMsgID (Subscribe msgid _)   = Just msgid
getMsgID (SubAck msgid _)      = Just msgid
getMsgID (Unsubscribe msgid _) = Just msgid
getMsgID (UnsubAck msgid)      = Just msgid
getMsgID PingReq               = Nothing
getMsgID PingResp              = Nothing
getMsgID Disconnect            = Nothing

-- | A topic is a \"hierarchical name space that defines a taxonomy of
-- information sources for which subscribers can register an interest.\"
-- See the
-- <http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#appendix-a specification>
-- for more details.
--
-- A topic can be inspected by using the 'matches' function or after using
-- 'getLevels', e.g.:
--
-- > f1 topic
-- >   | topic `matches` "mqtt/hs/example" = putStrLn "example"
-- >   | topic `matches` "mqtt/hs/#" = putStrLn "wildcard"
-- >
-- > f2 topic = case getLevels topic of
-- >              ["mqtt", "hs", "example"] -> putStrLn "example"
-- >              "mqtt" : "hs" : _ -> putStrLn "wildcard"
data Topic = Topic { levels :: [Text], orig :: Text }
-- levels and orig should always refer to the same topic, so no text has to be
-- copied when converting from/to text

instance Show Topic where
    show (Topic _ t) = show t

instance Eq Topic where
    Topic _ t1 == Topic _ t2 = t1 == t2

-- | Check if one of the 'Topic's matches the other, taking
-- <http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html#appendix-a wildcards>
-- into consideration.
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

-- | Reasons why connecting to a broker might fail.
data ConnectError
    = WrongProtocolVersion
    | IdentifierRejected
    | ServerUnavailable
    | BadLogin
    | Unauthorized
    | UnrecognizedReturnCode
    | InvalidResponse
    deriving (Show, Typeable)

instance Exception ConnectError where

-- | Convert a return code to a 'ConnectError'.
toConnectError :: Word8 -> ConnectError
toConnectError 1 = WrongProtocolVersion
toConnectError 2 = IdentifierRejected
toConnectError 3 = ServerUnavailable
toConnectError 4 = BadLogin
toConnectError 5 = Unauthorized
toConnectError _ = UnrecognizedReturnCode

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
