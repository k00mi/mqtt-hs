{-# Language OverloadedStrings #-}
module Pubalot where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Monad (unless, replicateM_)
import Data.ByteString (ByteString)
import Data.Foldable (for_)
import Network.MQTT

t :: Topic
t = "topic"
p :: ByteString
p = "payload"

n :: Int
n = 1000

main :: IO ()
main = do
  cmds <- mkCommands
  pub <- newTChanIO
  let mqtt = defaultConfig cmds pub

  _ <- forkIO $ do
    [Handshake] <- subscribe mqtt [(t, Handshake)]

    _ <- forkIO $ replicateM_ n $ publish mqtt Handshake False t p

    replicateM_ n $ do
      Message _h b <- atomically (readTChan pub)
      unless (topic b == t) $
        putStr "FAILED: topic b = " >> print (topic b)
      unless (payload b == p) $
        putStr "FAILED: payload b = " >> print (payload b)

    maybeMsg <- atomically (tryReadTChan pub)
    for_ maybeMsg $ \(Message h b) -> do
      putStrLn "FAILED: received unexpected message:"
      putStr "\tHeader: " >> print h
      putStrLn "\tPayload:"
      putStr "\t\tMsgID: " >> print (pubMsgID b)
      putStr "\t\tTopic: " >> print (topic b)
      putStr "\t\tPayload: " >> print (payload b)

    disconnect mqtt

  term <- run mqtt
  case term of
    UserRequested -> return ()
    _ -> putStrLn $ "FAILED: term = " ++ show term
