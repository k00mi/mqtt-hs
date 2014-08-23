{-# Language OverloadedStrings, RecordWildCards, GADTs #-}
{-|
Module: MQTT.Encoding
Copyright: Lukas Braun 2014
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

Binary encoding for MQTT messages.
-}

module Network.MQTT.Encoding where

import Data.Bits (Bits(..))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.ByteString.Builder
import Data.Foldable (foldMap)
import Data.Int (Int64)
import Data.Maybe (isJust)
import Data.Monoid ((<>), mconcat, mempty)
import Data.Word (Word8)
import Data.Text.Encoding (encodeUtf8)
import System.IO (Handle)

import Network.MQTT.Types

-- | Directly write a 'Message' to the buffer of a 'Handle'.
writeTo :: Handle -> Message t -> IO ()
writeTo h msg = hPutBuilder h (putMessage msg)

-- | Generate a 'Builder' for any 'Message'.
putMessage :: Message t -> Builder
putMessage Message{..} = mconcat
    [ putMqttHeader header (msgType body)
    , encodeRemaining remaining
    , lazyByteString bodyBS
    ]
  where
    -- the header contains the length of the remaining message, so we have
    -- to build the body to determine the length in bytes
    bodyBS = toLazyByteString (putBody body)
    remaining = BSL.length bodyBS


---------------------------------
-- * Fixed Header
---------------------------------

-- | Build a 'MqttHeader' for the given message type.
putMqttHeader :: MqttHeader -> Word8 -> Builder
putMqttHeader (Header dup qos retain) msgType =
    word8 $ shiftL msgType 4 .|.
            shiftL (toBit dup) 3 .|.
            shiftL (fromQoS qos) 1 .|.
            toBit retain

-- | Encode the remaining length field.
encodeRemaining :: Int64 -> Builder
encodeRemaining n =
    let (n', digit) = n `quotRem` 128
        digit' = fromIntegral digit
    in if n' > 0
         -- set top bit to indicate more digits are following
         then word8 (digit' .|. 0x80) <> encodeRemaining n'
         else word8 digit'


---------------------------------
-- * Body
---------------------------------

-- | Build the 'MessageBody' for any message type.
putBody :: MessageBody t -> Builder
putBody (MConnect connect)          = putConnect      connect
putBody (MConnAck connAck)          = putConnAck      connAck
putBody (MPublish publish)          = putPublish      publish
putBody (MPubAck simpleMsg)         = putSimple       simpleMsg
putBody (MPubRec simpleMsg)         = putSimple       simpleMsg
putBody (MPubRel simpleMsg)         = putSimple       simpleMsg
putBody (MPubComp simpleMsg)        = putSimple       simpleMsg
putBody (MSubscribe subscribe)      = putSubscribe    subscribe
putBody (MSubAck subAck)            = putSubAck       subAck
putBody (MUnsubscribe unsubscribe)  = putUnsubscribe  unsubscribe
putBody (MUnsubAck simpleMsg)       = putSimple       simpleMsg
putBody MPingReq                    = mempty
putBody MPingResp                   = mempty
putBody MDisconnect                 = mempty


putConnect :: Connect -> Builder
putConnect Connect{..} = mconcat
    [ putMqttText "MQIsdp" -- protocol
    , word8 3 -- version
    , word8 flags
    , word16BE keepAlive
    , putMqttText clientID
    , maybe mempty putTopic (fmap wTopic will)
    , maybePut (fmap wMsg will)
    , maybePut username
    , maybePut password
    ]
  where
    maybePut = maybe mempty putMqttText
    flags = shiftL (toBit (isJust username)) 7 .|.
            shiftL (toBit (isJust password)) 6 .|.
            shiftL (maybe 0 (toBit . wRetain) will) 5 .|.
            shiftL (maybe 0 (fromQoS . wQoS) will) 3 .|.
            shiftL (toBit (isJust will)) 2 .|.
            shiftL (toBit cleanSession) 1


putConnAck :: ConnAck -> Builder
putConnAck = word8 . returnCode


putPublish :: Publish -> Builder
putPublish Publish{..} = mconcat
    [ putTopic topic
    , maybe mempty putMsgID pubMsgID
    , byteString payload
    ]


putSubscribe :: Subscribe -> Builder
putSubscribe Subscribe{..} = mconcat
    [ putMsgID subscribeMsgID
    , foldMap (\(txt, qos) -> putTopic txt <> word8 (fromQoS qos)) subTopics
    ]


putSubAck :: SubAck -> Builder
putSubAck SubAck{..} = mconcat
    [ putMsgID subAckMsgID
    , foldMap (word8 . fromQoS) granted
    ]

putUnsubscribe :: Unsubscribe -> Builder
putUnsubscribe Unsubscribe{..} = mconcat
    [ putMsgID unsubMsgID
    , foldMap putTopic unsubTopics
    ]

putSimple :: SimpleMsg -> Builder
putSimple = putMsgID . msgID


---------------------------------
-- * Utility functions
---------------------------------

-- | Build a 'MsgID'.
putMsgID :: MsgID -> Builder
putMsgID = word16BE

-- | Build a length-prefixed 'MqttText'.
putMqttText :: MqttText -> Builder
putMqttText (MqttText text) = let utf = encodeUtf8 text in
    word16BE (fromIntegral (BS.length utf)) <> byteString utf

-- | Build a 'Topic'.
putTopic :: Topic -> Builder
putTopic = putMqttText . fromTopic

-- | Encode a 'QoS'.
fromQoS :: (Num a) => QoS -> a
fromQoS NoConfirm = 0
fromQoS Confirm   = 1
fromQoS Handshake = 2

-- | Convert a 'Bool' to 0 or 1.
toBit :: (Num a) => Bool -> a
toBit False = 0
toBit True = 1

-- | Encode the type of a 'MessageBody'.
msgType :: (Num a) => MessageBody t -> a
msgType (MConnect _)     = 1
msgType (MConnAck _)     = 2
msgType (MPublish _)     = 3
msgType (MPubAck _)      = 4
msgType (MPubRec _)      = 5
msgType (MPubRel _)      = 6
msgType (MPubComp _)     = 7
msgType (MSubscribe _)   = 8
msgType (MSubAck _)      = 9
msgType (MUnsubscribe _) = 10
msgType (MUnsubAck _)    = 11
msgType MPingReq         = 12
msgType MPingResp        = 13
msgType MDisconnect      = 14
