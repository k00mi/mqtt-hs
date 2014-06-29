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

import MQTT.Types

-- | Type of a parser that also keeps track of the remaining length.
type MessageParser a = StateT Word32 Parser a

-- | Parse any MQTT message.
message :: Parser Message
message = do
    header <- mqttHeader
    remaining <- parseRemaining
    body header remaining

body :: MqttHeader -> Word32 -> Parser Message
body header remaining =
    let combine varHeaderP payloadP = Message header <$> varHeaderP <*> payloadP
        noop = pure Nothing
        parser =
          case msgType header of
            CONNECT      -> do varHeader <- connectHeader
                               Message header (Just varHeader)
                                 <$> fmap Just (connectPL varHeader)
            CONNACK      -> combine connackHeader noop
            PUBLISH      -> combine (publishHeader header) (fmap Just publishPL)
            PUBACK       -> combine msgIDHeader noop
            PUBREC       -> combine msgIDHeader noop
            PUBREL       -> combine msgIDHeader noop
            PUBCOMP      -> combine msgIDHeader noop
            SUBSCRIBE    -> combine msgIDHeader (fmap Just subscribePL)
            SUBACK       -> combine msgIDHeader (fmap Just subackPL)
            UNSUBSCRIBE  -> combine msgIDHeader (fmap Just unsubscribePL)
            UNSUBACK     -> combine msgIDHeader noop
            PINGREQ      -> combine noop noop
            PINGRESP     -> combine noop noop
            DISCONNECT   -> combine noop noop
    in evalStateT parser remaining


---------------------------------
-- * Fixed Header
---------------------------------

-- | Parser for the fixed header part of a MQTT message.
mqttHeader :: Parser MqttHeader
mqttHeader = do
    byte1 <- anyWord8
    msgType <- toMsgType $ shiftR byte1 4
    qos <- toQoS $ 3 .&. shiftR byte1 1
    let retain = testBit byte1 0
        dup = testBit byte1 3
    return $ Header msgType dup qos retain

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

connectHeader :: MessageParser VarHeader
connectHeader = do
    protocol <- mqttText
    version <- anyWord8'
    flags <- anyWord8'
    let clean = testBit flags 1
        willFlag = testBit flags 2
        willRetain = if willFlag
                      then Just (testBit flags 5)
                      else Nothing
        usernameFlag = testBit flags 7
        passwordFlag = testBit flags 6
    willQoS <- if willFlag
                 then Just <$> toQoS (3 .&. shiftR flags 3)
                 else return Nothing
    keepAlive <- anyWord16BE
    return $ VHConnect $
      ConnectHeader
        protocol
        version
        clean
        ((,) <$> willQoS <*> willRetain)
        usernameFlag
        passwordFlag
        keepAlive

connackHeader :: MessageParser (Maybe VarHeader)
connackHeader = Just . VHConnAck <$> anyWord8'

publishHeader :: MqttHeader -> MessageParser (Maybe VarHeader)
publishHeader header =
    Just . VHPublish <$> (PublishHeader
                          <$> fmap toTopic mqttText
                          <*> if qos header > NoConfirm
                                then Just <$> msgID
                                else pure Nothing)

-- | Parse a variable header that consists solely of a message ID
msgIDHeader :: MessageParser (Maybe VarHeader)
msgIDHeader = Just . VHOther <$> msgID

---------------------------------
-- * Payload
---------------------------------

-- | Parses the fields of a connect message payload as indicated in the
-- variable header.
connectPL :: VarHeader -> MessageParser Payload
connectPL (VHConnect connHeader) = do
  before <- get
  clientID <- mqttText
  after <- get
  when (before - after + 2 > 23) $
    fail $ "Client ID must not be longer than 23 chars: " ++ show clientID
  PLConnect <$>
    (ConnectPL clientID
      <$> (fmap toTopic <$> textIfSet (isJust . will))
      <*> textIfSet (isJust . will)
      <*> textIfSet usernameFlag
      <*> textIfSet passwordFlag)
  where
    textIfSet field = if field connHeader
                        then Just <$> mqttText
                        else pure Nothing
connectPL varHeader =
    fail $ "Trying to parse CONNECT payload after non-CONNECT variable header: "
            ++ show varHeader

publishPL :: MessageParser Payload
publishPL = get >>= fmap PLPublish . take'

subscribePL :: MessageParser Payload
subscribePL = fmap PLSubscribe $
    whileM ((0 <) <$> get) $
      (,) <$> fmap toTopic mqttText <*> (anyWord8' >>= toQoS)

subackPL :: MessageParser Payload
subackPL = fmap PLSubAck $
    whileM ((0 <) <$> get) $
      anyWord8' >>= toQoS

unsubscribePL :: MessageParser Payload
unsubscribePL = PLUnsubscribe <$>
    whileM ((0 <) <$> get) (fmap toTopic mqttText)


---------------------------------
-- * Utility functions
---------------------------------

-- | Parse a length-prefixed UTF-8 string.
mqttText :: MessageParser MqttText
mqttText = do
    n <- anyWord16BE
    rslt <- decodeUtf8' <$> take' n
    case rslt of
      Left err -> fail $ "Invalid UTF-8: " ++ show err
      Right txt -> return $ MqttText txt

-- | Synonym for 'anyWord16BE'.
msgID :: MessageParser Word16
msgID = anyWord16BE

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

-- | Convert a number to a 'MsgType'. Calls 'fail' if the number can't be
-- converted.
toMsgType :: (Num a, Eq a, Show a, Monad m) => a -> m MsgType
toMsgType 1  = return CONNECT
toMsgType 2  = return CONNACK
toMsgType 3  = return PUBLISH
toMsgType 4  = return PUBACK
toMsgType 5  = return PUBREC
toMsgType 6  = return PUBREL
toMsgType 7  = return PUBCOMP
toMsgType 8  = return SUBSCRIBE
toMsgType 9  = return SUBACK
toMsgType 10 = return UNSUBSCRIBE
toMsgType 11 = return UNSUBACK
toMsgType 12 = return PINGREQ
toMsgType 13 = return PINGRESP
toMsgType 14 = return DISCONNECT
toMsgType x  = fail $ "Invalid message type: " ++ show x
