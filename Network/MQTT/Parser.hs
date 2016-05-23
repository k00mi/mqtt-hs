{-# Language OverloadedStrings, GADTs, DataKinds #-}
{-|
Module: MQTT.Parsers
Copyright: Lukas Braun 2014-2016
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

Parsers for MQTT messages.
-}
module Network.MQTT.Parser where

import Control.Monad
import Control.Monad.Loops
import Control.Monad.State.Strict
import Control.Applicative
import Data.Attoparsec.ByteString
import Data.Bits
import qualified Data.ByteString as BS
import Data.Text.Encoding (decodeUtf8With)
import Data.Text.Encoding.Error (lenientDecode)
import Data.Word
import Prelude hiding (takeWhile, take)

import Network.MQTT.Types hiding (body)

-- | Type of a parser that also keeps track of the remaining length.
type MessageParser a = StateT Word32 Parser a

-- | Parse any MQTT message.
message :: Parser SomeMessage
message = do
    (msgType, header) <- mqttHeader
    remaining <- parseRemaining
    msg <- withSomeSingI msgType $ \sMsgType ->
      SomeMessage . Message header <$> mqttBody header sMsgType remaining
    return msg


---------------------------------
-- * Fixed Header
---------------------------------

-- | Parser for the fixed header part of a MQTT message.
mqttHeader :: Parser (MsgType, MqttHeader)
mqttHeader = ctxt "mqttHeader" $ do
    byte1 <- anyWord8
    qos <- toQoS $ 3 .&. shiftR byte1 1
    let retain = testBit byte1 0
        dup = testBit byte1 3
        msgType = shiftR byte1 4
    msgType' <- case msgType of
                  1  -> return CONNECT
                  2  -> return CONNACK
                  3  -> return PUBLISH
                  4  -> return PUBACK
                  5  -> return PUBREC
                  6  -> return PUBREL
                  7  -> return PUBCOMP
                  8  -> return SUBSCRIBE
                  9  -> return SUBACK
                  10 -> return UNSUBSCRIBE
                  11 -> return UNSUBACK
                  12 -> return PINGREQ
                  13 -> return PINGRESP
                  14 -> return DISCONNECT
                  x  -> fail $ "Invalid message type: " ++ show x
    return (msgType', Header dup qos retain)

-- | Parse the 'remaining length' field that indicates how long the rest of
-- the message is.
parseRemaining :: Parser Word32
parseRemaining = ctxt "parseRemaining" $ do
    bytes <- takeWhile (> 0x7f) -- bytes with first bit set
    when (BS.length bytes > 3) $
      fail "'Remaining length' field must not be longer than 4 bytes"
    stopByte <- anyWord8
    let (factor, acc) = BS.foldl' f (1, 0) bytes
    return $ acc + factor * fromIntegral stopByte
  where
    f (factor, acc) byte =
      (factor*128, acc + factor * fromIntegral (0x7f .&. byte))


---------------------------------
-- * Message Body
---------------------------------

-- | «@mqttBody header msgtype remaining@» parses a 'Message' of type
-- @msgtype@ that is @remaining@ bytes long.
mqttBody :: MqttHeader -> SMsgType t -> Word32 -> Parser (MessageBody t)
mqttBody header msgType remaining = ctxt "mqttBody" $
    let parser =
          case msgType of
            SCONNECT     -> connect
            SCONNACK     -> connAck
            SPUBLISH     -> publish header
            SPUBACK      -> PubAck  <$> parseMsgID
            SPUBREC      -> PubRec  <$> parseMsgID
            SPUBREL      -> PubRel  <$> parseMsgID
            SPUBCOMP     -> PubComp <$> parseMsgID
            SSUBSCRIBE   -> subscribe
            SSUBACK      -> subAck
            SUNSUBSCRIBE -> unsubscribe
            SUNSUBACK    -> UnsubAck <$> parseMsgID
            SPINGREQ     -> pure PingReq
            SPINGRESP    -> pure PingResp
            SDISCONNECT  -> pure Disconnect
    in evalStateT parser remaining

connect :: MessageParser (MessageBody 'CONNECT)
connect = ctxt' "connect" $ do
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
                  <*> (ctxt' "Will Topic" $ fmap toTopic mqttText)
                  <*> (ctxt' "Will Message" mqttText)

    username <- ctxt' "Username" $ parseIf usernameFlag mqttText

    password <- ctxt' "Password" $ parseIf passwordFlag mqttText

    return $ Connect clean mWill clientID username password keepAlive
  where
    protocol = ctxt' "protocol" $ do
      prot <- mqttText
      when (prot /= "MQIsdp") $
        fail $ "Invalid protocol: " ++ show prot

    version = ctxt' "version" $ do
      version <- anyWord8'
      when (version /= 3) $
        fail $ "Invalid version: " ++ show version

    getClientID = ctxt' "getClientID" $ do
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

connAck :: MessageParser (MessageBody 'CONNACK)
connAck = ctxt' "connAck" $ anyWord8' {- reserved -} *> (ConnAck <$> anyWord8')

publish :: MqttHeader -> MessageParser (MessageBody 'PUBLISH)
publish header = ctxt' "publish" $ Publish
                  <$> getTopic
                  <*> (if qos header > NoConfirm
                         then Just <$> parseMsgID
                         else return Nothing)
                  <*> (get >>= take')

subscribe :: MessageParser (MessageBody 'SUBSCRIBE)
subscribe = ctxt' "subscribe" $ Subscribe
              <$> parseMsgID
              <*> whileM ((0 <) <$> get)
                    ((,) <$> getTopic <*> (anyWord8' >>= toQoS))

subAck :: MessageParser (MessageBody 'SUBACK)
subAck = ctxt' "subAck" $ SubAck
          <$> parseMsgID
          <*> whileM ((0 <) <$> get) (anyWord8' >>= toQoS)

unsubscribe :: MessageParser (MessageBody 'UNSUBSCRIBE)
unsubscribe = ctxt' "unsubscribe" $ Unsubscribe
                <$> parseMsgID
                <*> whileM ((0 <) <$> get) getTopic


---------------------------------
-- * Utility functions
---------------------------------

-- | Parse a topic name.
getTopic :: MessageParser Topic
getTopic = ctxt' "getTopic" $ toTopic <$> mqttText

-- | Parse a length-prefixed UTF-8 string.
mqttText :: MessageParser MqttText
mqttText = ctxt' "mqttText" $
    MqttText . decodeUtf8With lenientDecode <$> (anyWord16BE >>= take')

-- | Synonym for 'anyWord16BE'.
parseMsgID :: MessageParser Word16
parseMsgID = ctxt' "parseMsgID" anyWord16BE

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

ctxt :: String -> Parser a -> Parser a
ctxt = flip (<?>)

ctxt' :: String -> MessageParser a -> MessageParser a
ctxt' = mapStateT . ctxt

-- | A lifted version of attoparsec's 'take' that also subtracts the
-- length.
take' :: Word32 -> MessageParser BS.ByteString
take' n = parseLength n >> lift (take (fromIntegral n))

-- | Subtract 'n' from the remaining length or 'fail' if there is not
-- enough input left.
parseLength :: Word32 -> MessageParser ()
parseLength n = do
    rem <- get
    if rem < n
      then fail "Reached remaining = 0 before end of message."
      else put $ rem - n

-- | Convert a number to a 'QoS'. 'fail' if the number can't be converted.
toQoS :: (Num a, Eq a, Show a, Monad m) => a -> m QoS
toQoS 0 = return NoConfirm
toQoS 1 = return Confirm
toQoS 2 = return Handshake
toQoS x = fail $ "Invalid QoS value: " ++ show x
