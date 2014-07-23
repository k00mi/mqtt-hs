{-|
Module: MQTT.Logger
Copyright: Lukas Braun 2014
License: GPL-3
Maintainer: koomi+mqtt@hackerspace-bamberg.de

A simple logger abstraction.
-}
module MQTT.Logger where

import System.IO

-- | Absract logger with three levels of importance.
data Logger
    = Logger
        { logInfo :: String -> IO ()
        , logWarning :: String -> IO ()
        , logError :: String -> IO ()
        }

-- | 'logInfo' prints to stdout, 'logWarning' and 'logError' to stderr
-- (with [Warning]/[Error] prefix)
stdLogger :: Logger
stdLogger = Logger
              putStrLn
              (\msg -> hPutStrLn stderr $ "[Warning] " ++ msg)
              (\msg -> hPutStrLn stderr $ "[Error] " ++ msg)

-- | Log only warnings and errors, ignoring anything passed to 'logInfo'.
warnings :: Logger -> Logger
warnings l = l { logInfo = ignore }

-- | Like 'warnings', but logs only errors.
errors :: Logger -> Logger
errors l = l { logInfo = ignore, logWarning = ignore }

-- | Ignore the message.
ignore :: String -> IO ()
ignore _ = return ()
