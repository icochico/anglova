package conn

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/elodina/go_kafka_client/Godeps/_workspace/src/github.com/Shopify/sarama"
	"github.com/garyburd/redigo/redis"
	"github.com/ipfs/go-ipfs-api"
	"github.com/nats-io/go-nats"
	zmq "github.com/pebbe/zmq4"
	"github.com/streadway/amqp"
	"ihmc.us/anglova/protocol"
	"sync"
	"time"
)

type Kafka struct {
	Producer sarama.SyncProducer
	Consumer sarama.Consumer
}

type ZMQ struct {
	ctx *zmq.Context
	Pub *zmq.Socket
	Sub *zmq.Socket
}

// Redis client - defines the underlying connection and pub-sub
// connections, as well as a mutex for locking write access,
// since this occurs from multiple goroutines.
type Redis struct {
	Conn redis.Conn
	redis.PubSubConn
	sync.Mutex
}

type Conn struct {
	Protocol string
	//NATS works on the conn
	NATSClient nats.Conn
	//RabbitMQ works on the exchange, the exchange is bind to the conn
	//to make the code interoperable when ConnType is RABBITMQ a channel will be return
	RabbitMQClient amqp.Channel
	//MQTTLib client
	MQTTClient mqtt.Client
	//kafka client
	KafkaClient Kafka
	//IPFS shell
	IPFSClient shell.Shell
	//ZMQ client
	ZMQClient ZMQ
	//Redis client
	RedisClient Redis
}

func New(proto string, host string, port string, topic string) (*Conn, error) {
	switch proto {
	case protocol.RabbitMQ:
		conn, err := amqp.Dial(proto + "://guest:guest@" + host + ":" + port + "/")
		uri := proto + "://guest:guest@" + host + ":" + port
		log.Info("URI to connect %s\n", uri)
		if err != nil {
			return nil, err
		}
		//defer conn.Close()
		channel, err := conn.Channel()
		if err != nil {
			return nil, err
		}
		//defer channel.Close()
		err = channel.ExchangeDeclare(
			topic,    // name
			"fanout", // type
			true,     // durable
			false,    // auto-deleted
			false,    // internal
			false,    // no-wait
			nil,      // arguments
		)
		if err != nil {
			return nil, err
		}
		return &Conn{Protocol: proto, RabbitMQClient: *channel}, nil
	case protocol.NATS:
		nconn, err := nats.Connect(proto + "://" + host + ":" + port)
		if err != nil {
			return nil, errors.New("Unable to establish a connection with NATS broker")
		}
		return &Conn{Protocol: proto, NATSClient: *nconn}, nil
	case protocol.MQTT:
		opts := mqtt.NewClientOptions().AddBroker("tcp://" + host + ":" + port)
		mqttConn := mqtt.NewClient(opts)
		if token := mqttConn.Connect(); token.Wait() && token.Error() != nil {
			return nil, errors.New("Unable to establish a connection with MQTTLib broker")
		}
		return &Conn{Protocol: proto, MQTTClient: mqttConn}, nil
	case protocol.Kafka:
		var kafkaAddresses = []string{host + ":" + port}
		config := sarama.NewConfig()
		config.Consumer.Fetch.Default = 110000
		config.Consumer.Fetch.Max = 10000000
		config.ChannelBufferSize = 1000000
		con, err := sarama.NewConsumer(kafkaAddresses, config)
		config = sarama.NewConfig()
		config.Producer.Partitioner = sarama.NewRandomPartitioner
		config.Producer.RequiredAcks = sarama.WaitForAll
		config.Producer.MaxMessageBytes = 10000000
		config.ChannelBufferSize = 1000000
		prod, err := sarama.NewSyncProducer(kafkaAddresses, config)

		client := &Kafka{Producer: prod, Consumer: con}

		if err != nil {
			return nil, errors.New("Unable to establish a connection with kafka broker")
		}
		return &Conn{Protocol: proto, KafkaClient: *client}, nil

	case protocol.IPFS:
		client := shell.NewShell(proto + "://" + host + ":" + port)
		return &Conn{Protocol: proto, IPFSClient: *client}, nil
	case protocol.ZMQ:
		var err error
		var context *zmq.Context
		context, err = zmq.NewContext()
		if err != nil {
			return nil, errors.New("Unable to establish a connection with ZMQ broker")
		}
		var pub *zmq.Socket
		pub, err = context.NewSocket(zmq.PUSH)
		if err != nil {
			return nil, errors.New("Unable to establish a connection with ZMQ broker")
		}
		pub.Connect(fmt.Sprintf("tcp://%s:%s", host, port))
		var sub *zmq.Socket
		sub, err = context.NewSocket(zmq.SUB)
		if err != nil {
			return nil, errors.New("Unable to establish a connection with ZMQ broker")
		}
		sub.Connect(fmt.Sprintf("tcp://%s:%s", host, port))
		client := &ZMQ{context, pub, sub}

		return &Conn{Protocol: proto, ZMQClient: *client}, nil
	case protocol.Redis:
		var redisHost = fmt.Sprintf("%s:%s", host, port)
		conn, _ := redis.Dial("tcp", redisHost)
		pubsub, _ := redis.Dial("tcp", redisHost)
		client := Redis{conn, redis.PubSubConn{pubsub}, sync.Mutex{}}
		go func() {
			for {
				time.Sleep(200 * time.Millisecond)
				client.Lock()
				client.Conn.Flush()
				client.Unlock()
			}
		}()
		return &Conn{Protocol: proto, RedisClient: client}, nil
	default:
		return nil, errors.New("No ConnType speficied")
	}
}
