{-# Language OverloadedStrings #-}
module SubPub where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad (unless)
import Data.ByteString (ByteString)
import Network.MQTT

t :: Topic
t = "topic"
p :: ByteString
p = "payload"

main :: IO ()
main = do
  cmds <- mkCommands
  pub <- newTChanIO
  let mqtt = defaultConfig cmds pub

  _ <- forkIO $ do
    [Handshake] <- subscribe mqtt [(t, Handshake)]
    publish mqtt Handshake False t p
    Message _h b <- atomically (readTChan pub)
    unless (topic b == t) $
      putStr "FAILED: topic b = " >> print (topic b)
    unless (payload b == p) $
      putStr "FAILED: payload b = " >> print (payload b)
    disconnect mqtt

  term <- run mqtt
  case term of
    UserRequested -> return ()
    _ -> putStrLn $ "FAILED: term = " ++ show term
