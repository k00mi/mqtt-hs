{-|
Module: MQTT.Logger
Copyright: Lukas Braun 2014
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

A simple logger abstraction.
-}
module Network.MQTT.Logger where

import System.IO

-- | Absract logger with four priority levels.
data Logger
    = Logger
        { logDebug :: String -> IO ()
        , logInfo :: String -> IO ()
        , logWarning :: String -> IO ()
        , logError :: String -> IO ()
        }

-- | 'logInfo' and 'logDebug' print to stdout, 'logWarning' and 'logError' to
-- stderr. A prefix indicating the level is prepended to each message.
stdLogger :: Logger
stdLogger = Logger
              (\msg -> putStrLn $ "[Debug] " ++ msg)
              (\msg -> putStrLn $ "[Info] " ++ msg)
              (\msg -> hPutStrLn stderr $ "[Warning] " ++ msg)
              (\msg -> hPutStrLn stderr $ "[Error] " ++ msg)

-- | Don't log debug messages.
info :: Logger -> Logger
info l = l { logDebug = ignore }

-- | Log only warnings and errors.
warnings :: Logger -> Logger
warnings l = (info l) { logInfo = ignore }

-- | Log only errors.
errors :: Logger -> Logger
errors l = (warnings l) { logWarning = ignore }

-- | Ignore the message.
ignore :: String -> IO ()
ignore _ = return ()
