{-# Language OverloadedStrings, GADTs #-}
module Pubalot where

import Control.Concurrent
import Control.Concurrent.STM
import Data.ByteString (ByteString)
import Data.Word (Word16)
import Network.MQTT
import Network.MQTT.Internal
import System.Timeout (timeout)

t :: Topic
t = "topic"
p :: ByteString
p = "payload"

tout :: Word16
tout = 2 -- seconds

main :: IO ()
main = do
  cmds <- newTChanIO
  pub <- newTChanIO
  let mqtt = (defaultConfig (Cmds cmds) pub) { cKeepAlive = Just tout }

  _ <- forkIO $ do
    -- dup cmds, publish, wait for timeout, check for keepalive msg
    cmds' <- atomically $ dupTChan cmds

    publish mqtt NoConfirm False t p
    CmdSend (SomeMessage (Message _h (Publish{}))) <- atomically (readTChan cmds')

    maybeAwait <- timeout (fromIntegral tout * 2 * 10^6) $
                    atomically (readTChan cmds')
    case maybeAwait of
      Nothing -> putStrLn "FAILED: no CmdAwait within timeout"
      Just (CmdAwait _) -> return ()
      Just (CmdSend _) -> putStrLn "FAILED: unexpected CmdSend"
      Just (CmdStopWaiting _) -> putStrLn "FAILED: unexpected CmdStopWaiting"
      Just CmdDisconnect -> putStrLn "FAILED: unexpected CmdDisconnect"

    -- timeout? tryRead?
    cmdSend <- atomically $ readTChan cmds'
    case cmdSend of
      (CmdSend (SomeMessage (Message _h PingReq))) -> return ()
      (CmdSend (SomeMessage _)) -> putStrLn "FAILED: unexpected message"
      (CmdAwait _) -> putStrLn "FAILED: unexpected CmdAwait"
      (CmdStopWaiting _) -> putStrLn "FAILED: unexpected CmdStopWaiting"
      CmdDisconnect -> putStrLn "FAILED: unexpected CmdDisconnect"
    disconnect mqtt

  term <- run mqtt
  case term of
    UserRequested -> return ()
    _ -> putStrLn $ "FAILED: term = " ++ show term
