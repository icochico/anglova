```  
   _ _  _ __    __ _ | |  ___  __   __   _ _ 
 / _` || '_ \  / _` || | / _ \ \ \ / / / _` |
| (_| || | | || (_| || || (_) | \ V / | (_| |
 \__,_||_| |_| \__, ||_| \___/   \_/   \__,_|
               |___/
```

<b>Anglova</b> is a benchmark platform that enables the evaluation
of a variety of messaging protocols and P2P file systems:
NATS, RabbitMQ, Apache Kafka, MQTT, IPFS etc.
			   


<b>Dependencies</b>

Go (>= 1.8) https://golang.org/dl/<br/>
GNU make (suggested) https://www.gnu.org/software/make/<br/>

<b>Build</b>

With GNU make:

```make```

Without make:

```go get ihmc.us/anglova``` <br/>
```go install ihmc.us/anglova``` <br/>

<b>Run</b>

```Usage:
  anglova [command]

Available Commands:
  help        Help about any command
  publish     Publish to a topic
  pubsub      Publish and subscribe to a topic
  scenario    Run the anglova scenario
  stats       Run the stats server
  subscribe   Subscribe to a topic
  version     Print the version number of anglova

Flags:
      --broker-address string       broker address (default "127.0.0.1")
      --config string               config file (default is $HOME/.anglova.yaml)
      --enable-blueforce            Enable Blue Force Tracks in scenario, topic: blueforce
      --enable-hqreport             Enable HQ Reports in scenario, topic: hqreport
      --enable-sensordata           Enable Sensor Data in scenario, topic: sensordata
      --message-number int          number of messages (default 1024)
      --message-size int            size of the message (default 1024)
      --port string                 port used by the broker (default "4222")
      --protocol string             protocol type (default "nats")
      --publish-interval duration   interval between two messages published (default 1µs)
      --stats-address string        stats server address (default "127.0.0.1")
      --stats-port string           port used by the stats server (default "4223")
  -t, --toggle                      Help message for toggle
      --topic string                topic name (default "test")
      --viper                       Use Viper for configuration (default true)

Use "anglova [command] --help" for more information about a command.```

