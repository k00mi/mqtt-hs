{-# Language OverloadedStrings #-}
{-|
Module: MQTT.Parsers
Copyright: Lukas Braun 2014
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

Parsers for MQTT messages.
-}
module MQTT.Parser where

import Control.Monad
import Control.Monad.Loops
import Control.Monad.State.Strict
import Control.Applicative
import Data.Attoparsec.ByteString
import Data.Bits
import Data.Maybe (isJust)
import qualified Data.ByteString as BS
import Data.Text.Encoding (decodeUtf8')
import Data.Word
import Prelude hiding (takeWhile, take)

import MQTT.Types hiding (body)

-- | Type of a parser that also keeps track of the remaining length.
type MessageParser a = StateT Word32 Parser a

-- | Parse any MQTT message.
message :: Parser SomeMessage
message = do
    (msgType, header) <- mqttHeader
    remaining <- parseRemaining
    mqttBody header msgType remaining

mqttBody :: MqttHeader -> Word8 -> Word32 -> Parser SomeMessage
mqttBody header msgType remaining =
    let parser =
          case msgType of
            1  -> SomeMessage . Message header . MConnect     <$> connect
            2  -> SomeMessage . Message header . MConnAck     <$> connAck
            3  -> SomeMessage . Message header . MPublish     <$> publish header
            4  -> SomeMessage . Message header . MPubAck      <$> simpleMsg
            5  -> SomeMessage . Message header . MPubRec      <$> simpleMsg
            6  -> SomeMessage . Message header . MPubRel      <$> simpleMsg
            7  -> SomeMessage . Message header . MPubComp     <$> simpleMsg
            8  -> SomeMessage . Message header . MSubscribe   <$> subscribe
            9  -> SomeMessage . Message header . MSubAck      <$> subAck
            10 -> SomeMessage . Message header . MUnsubscribe <$> unsubscribe
            11 -> SomeMessage . Message header . MUnsubAck    <$> simpleMsg
            12 -> pure $ SomeMessage (Message header MPingReq)
            13 -> pure $ SomeMessage (Message header MPingResp)
            14 -> pure $ SomeMessage (Message header MDisconnect)
            t  -> lift $ fail ("Invalid message type: " ++ show t)
    in evalStateT parser remaining


---------------------------------
-- * Fixed Header
---------------------------------

-- | Parser for the fixed header part of a MQTT message.
mqttHeader :: Parser (Word8, MqttHeader)
mqttHeader = do
    byte1 <- anyWord8
    qos <- toQoS $ 3 .&. shiftR byte1 1
    let retain = testBit byte1 0
        dup = testBit byte1 3
        msgType = shiftR byte1 4
    return (msgType, Header dup qos retain)

-- | Parse the 'remaining length' field that indicates how long the rest of
-- the message is.
parseRemaining :: Parser Word32
parseRemaining = do
    bytes <- takeWhile (> 0x7f) -- bytes with first bit set
    when (BS.length bytes > 3) $
      fail "'Remaining length' field must not be longer than 4 bytes"
    stopByte <- anyWord8
    return $ snd $ BS.foldr' f (128, fromIntegral stopByte) bytes
  where
    f byte (factor, acc) =
      (factor*128, acc + factor * fromIntegral (0x7f .&. byte))


---------------------------------
-- * Variable Headers
---------------------------------

connect :: MessageParser Connect
connect = do
    protocol
    version

    flags <- anyWord8'
    let clean = testBit flags 1
        willFlag = testBit flags 2
        usernameFlag = testBit flags 7
        passwordFlag = testBit flags 6

    keepAlive <- anyWord16BE

    clientID <- getClientID

    mWill <- parseIf willFlag $
               Will (testBit flags 5)
                  <$> toQoS (3 .&. shiftR flags 3)
                  <*> fmap toTopic mqttText
                  <*> mqttText

    username <- parseIf usernameFlag mqttText

    password <- parseIf passwordFlag mqttText

    return $ Connect clean mWill clientID username password keepAlive
  where
    protocol = do
      prot <- mqttText
      when (prot /= "MQIsdp") $
        fail $ "Invalid protocol: " ++ show prot

    version = do
      version <- anyWord8'
      when (version /= 3) $
        fail $ "Invalid version: " ++ show version

    getClientID = do
      before <- get
      clientID <- mqttText
      after <- get
      let len = before - after - 2 -- 2 for length prefix
      when (len > 23) $
        fail $ "Client ID must not be longer than 23 chars: "
                  ++ show (text clientID) ++ " (" ++ show len ++ ")"
      return clientID

    parseIf :: Applicative f => Bool -> f a -> f (Maybe a)
    parseIf flag parser = if flag then Just <$> parser else pure Nothing


connAck :: MessageParser ConnAck
connAck = ConnAck <$> anyWord8'

publish :: MqttHeader -> MessageParser Publish
publish header = Publish
                  <$> getTopic
                  <*> (if qos header > NoConfirm
                         then Just <$> parseMsgID
                         else return Nothing)
                  <*> (get >>= take')

subscribe :: MessageParser Subscribe
subscribe = Subscribe
              <$> parseMsgID
              <*> whileM ((0 <) <$> get)
                    ((,) <$> getTopic <*> (anyWord8' >>= toQoS))

subAck :: MessageParser SubAck
subAck = SubAck
          <$> parseMsgID
          <*> whileM ((0 <) <$> get) (anyWord8' >>= toQoS)

unsubscribe :: MessageParser Unsubscribe
unsubscribe = Unsubscribe
                <$> parseMsgID
                <*> whileM ((0 <) <$> get) getTopic

simpleMsg :: MessageParser SimpleMsg
simpleMsg = SimpleMsg <$> parseMsgID


---------------------------------
-- * Utility functions
---------------------------------

-- | Parse a topic name.
getTopic :: MessageParser Topic
getTopic = toTopic <$> mqttText

-- | Parse a length-prefixed UTF-8 string.
mqttText :: MessageParser MqttText
mqttText = do
    n <- anyWord16BE
    rslt <- decodeUtf8' <$> take' n
    case rslt of
      Left err -> fail $ "Invalid UTF-8: " ++ show err
      Right txt -> return $ MqttText txt

-- | Synonym for 'anyWord16BE'.
parseMsgID :: MessageParser Word16
parseMsgID = anyWord16BE

-- | Parse a big-endian 16bit integer.
anyWord16BE :: (Num a, Bits a) => MessageParser a
anyWord16BE = do
    msb <- anyWord8'
    lsb <- anyWord8'
    return $ shiftL (fromIntegral msb) 8 .|. fromIntegral lsb

-- | A lifted version of attoparsec's 'anyWord8' that also subtracts 1 from
-- the remaining length.
anyWord8' :: MessageParser Word8
anyWord8' = parseLength 1 >> lift anyWord8

-- | A lifted version of attoparsec's 'take' that also subtracts the
-- length.
take' :: Word32 -> MessageParser BS.ByteString
take' n = parseLength n >> lift (take (fromIntegral n))

-- | Subtract 'n' from the remaining length or 'fail' if there is not
-- enough left.
parseLength :: Word32 -> MessageParser ()
parseLength n = do
    rem <- get
    if rem < n
      then fail "Reached remaining = 0 before end of message."
      else put $ rem - n

-- | Convert a number to a 'QoS'. Calls 'fail' if the number can't be
-- converted.
toQoS :: (Num a, Eq a, Show a, Monad m) => a -> m QoS
toQoS 0 = return NoConfirm
toQoS 1 = return Confirm
toQoS 2 = return Handshake
toQoS x = fail $ "Invalid QoS value: " ++ show x
