mqtt-hs
=======

A Haskell MQTT client library.

A simple example, assuming a broker is running on localhost (needs -XOverloadedStrings):

```haskell
import Network.MQTT
import Network.MQTT.Logger
Just mqtt <- connect defaultConfig { cLogger = warnings stdLogger }
let f t payload = putStrLn $ "A message was published to " ++ show t ++ ": " ++ show pyload
subscribe mqtt NoConfirm "#" f
publish mqtt Handshake False "some random/topic" "Some content!"
```

See the haddock for more documentation.
