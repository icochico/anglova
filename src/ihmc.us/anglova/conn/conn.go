package conn

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"github.com/nats-io/go-nats"
	"github.com/streadway/amqp"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/elodina/go_kafka_client/Godeps/_workspace/src/github.com/Shopify/sarama"
	"ihmc.us/anglova/protocol"
)

type KafkaClient struct {
	Producer sarama.SyncProducer
	Consumer sarama.Consumer
}

type Conn struct {
	Protocol string
	//NATS works on the conn
	Nats nats.Conn
	//RabbitMQ works on the exchange, the exchange is bind to the conn
	//to make the code interoperable when ConnType is RABBITMQ a channel will be return
	Rabbitmq amqp.Channel
	//MQTTLib client
	MQTTClient mqtt.Client
	//kafka client
	Kafka KafkaClient
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
		return &Conn{Protocol: proto, Rabbitmq: *channel}, nil
	case protocol.NATS:
		nconn, err := nats.Connect(proto + "://" + host + ":" + port)
		if err != nil {
			return nil, errors.New("Unable to establish a connection with NATS broker")
		}
		return &Conn{Protocol: proto, Nats: *nconn}, nil
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

		client := &KafkaClient{Producer: prod, Consumer: con}

		if err != nil {
			return nil, errors.New("Unable to establish a connection with kafka broker")
		}
		return &Conn{Protocol: proto, Kafka: *client}, nil
	default:
		return nil, errors.New("No ConnType speficied")
	}
}
