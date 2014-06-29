{-# Language RecordWildCards #-}
{-|
Module: MQTT.Encoding
Copyright: Lukas Braun 2014
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

Binary encoding for MQTT messages.
-}

module MQTT.Encoding where

import Data.Bits (Bits(..))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.ByteString.Builder
import Data.Foldable (foldMap)
import Data.Int (Int64)
import Data.Maybe (fromMaybe, isJust)
import Data.Monoid ((<>), mconcat, mempty)
import Data.Text.Encoding (encodeUtf8)
import System.IO (Handle)

import MQTT.Types

writeTo :: Handle -> Message -> IO ()
writeTo h msg = hPutBuilder h (putMessage msg)

putMessage :: Message -> Builder
putMessage Message{..} = mconcat
    [ putMqttHeader header
    , encodeRemaining remaining
    , lazyByteString body
    ]
  where
    -- the header contains the length of the remaining message, so we have
    -- to build the body to determine the length in bytes
    body = toLazyByteString
             (fromMaybe mempty
               (fmap putVarHeader varHeader <> fmap putPayload payload))
    remaining = BSL.length body


---------------------------------
-- * Fixed Header
---------------------------------

putMqttHeader :: MqttHeader -> Builder
putMqttHeader (Header msgType dup qos retain) =
    word8 $ shiftL (fromMsgType msgType) 4 .|.
            shiftL (toBit dup) 3 .|.
            shiftL (fromQoS qos) 1 .|.
            toBit retain

encodeRemaining :: Int64 -> Builder
encodeRemaining n =
    let (n', digit) = n `quotRem` 128
        digit' = fromIntegral digit
    in if n' > 0
         -- set top bit to indicate more digits are following
         then word8 (digit' .|. 0x80) <> encodeRemaining n'
         else word8 digit'


---------------------------------
-- * Variable Headers
---------------------------------

putVarHeader :: VarHeader -> Builder
putVarHeader (VHConnect connectHeader) = putConnHeader connectHeader
putVarHeader (VHConnAck word) = word8 word
putVarHeader (VHPublish publishHeader) = putPublishHeader publishHeader
putVarHeader (VHOther msgID) = word16BE msgID

putConnHeader :: ConnectHeader -> Builder
putConnHeader (ConnectHeader{..}) = mconcat
    [ putMqttText protocolName
    , word8 protocolVersion
    , word8 flags
    , word16BE keepAlive
    ]
  where
    flags = shiftL (toBit usernameFlag) 7 .|.
            shiftL (toBit passwordFlag) 6 .|.
            shiftL (maybe 0 (toBit . snd) will) 5 .|.
            shiftL (maybe 0 (fromQoS . fst) will) 3 .|.
            shiftL (toBit (isJust will)) 2 .|.
            shiftL (toBit cleanSession) 1

putPublishHeader :: PublishHeader -> Builder
putPublishHeader PublishHeader{..} =
    putTopic topic <> maybe mempty word16BE messageID


---------------------------------
-- * Payload
---------------------------------

putPayload :: Payload -> Builder
putPayload (PLConnect connectPL)  = putConnectPL connectPL
putPayload (PLPublish blob)       = byteString blob
putPayload (PLSubscribe pairs)    = putSubscribePL pairs
putPayload (PLSubAck qoss)        = putSubAckPL qoss
putPayload (PLUnsubscribe topics) = putUnsubscribePL topics

putConnectPL :: ConnectPL -> Builder
putConnectPL ConnectPL{..} = mconcat
    [ putMqttText clientID
    , maybe mempty putTopic willTopic
    , maybePut willMsg
    , maybePut username
    , maybePut password
    ]
  where
    maybePut = maybe mempty putMqttText

putSubscribePL :: [(Topic, QoS)] -> Builder
putSubscribePL = foldMap $ \(txt, qos) ->
    putTopic txt <> word8 (fromQoS qos)

putSubAckPL :: [QoS] -> Builder
putSubAckPL = foldMap (word8 . fromQoS)

putUnsubscribePL :: [Topic] -> Builder
putUnsubscribePL = foldMap putTopic


---------------------------------
-- * Utility functions
---------------------------------

putMqttText :: MqttText -> Builder
putMqttText (MqttText text) = let utf = encodeUtf8 text in
    word16BE (fromIntegral (BS.length utf)) <> byteString utf

putTopic :: Topic -> Builder
putTopic = putMqttText . fromTopic

fromQoS :: (Num a) => QoS -> a
fromQoS NoConfirm = 0
fromQoS Confirm   = 1
fromQoS Handshake = 2

fromMsgType :: (Num a) => MsgType -> a
fromMsgType CONNECT     = 1
fromMsgType CONNACK     = 2
fromMsgType PUBLISH     = 3
fromMsgType PUBACK      = 4
fromMsgType PUBREC      = 5
fromMsgType PUBREL      = 6
fromMsgType PUBCOMP     = 7
fromMsgType SUBSCRIBE   = 8
fromMsgType SUBACK      = 9
fromMsgType UNSUBSCRIBE = 10
fromMsgType UNSUBACK    = 11
fromMsgType PINGREQ     = 12
fromMsgType PINGRESP    = 13
fromMsgType DISCONNECT  = 14

toBit :: (Num a) => Bool -> a
toBit False = 0
toBit True = 1
