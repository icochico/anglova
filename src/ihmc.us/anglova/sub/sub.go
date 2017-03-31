package sub

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"math/rand"
	"time"
	"fmt"
	"strings"
	"github.com/nats-io/go-nats"
	"github.com/eclipse/paho.mqtt.golang"
	_"github.com/Shopify/sarama"
	"ihmc.us/anglova/protocol"
	"ihmc.us/anglova/conn"
	"ihmc.us/anglova/msg"
	"ihmc.us/anglova/stats"
	"github.com/golang/protobuf/proto"
)

type Sub struct {
	Protocol string
	conn     conn.Conn
	SubID    uint32
	Topic    string
}

func New(protocol string, host string, port string, topic string) (*Sub, error) {

	subId := rand.Uint32() + uint32(time.Now().Nanosecond())
	conn, err := conn.New(protocol, host, port, topic)
	if err != nil {
		log.Error("Error during the connection with the broker!")
		return nil, err
	}
	return &Sub{Protocol: protocol,
		conn:         *conn,
		SubID:        subId,
		Topic:        topic}, nil
}

var fhand mqtt.MessageHandler = func(client mqtt.Client, message mqtt.Message) {
	fmt.Printf("Received message on topic: %s\nMessage: %s\n", message.Topic(), message.Payload())
}

func (subscriber Sub) CollectStats(topic string) error {

	log.Info("<- Stats Server Mode ->")

	quit := make(chan bool)
	subscriber.conn.Nats.Subscribe(topic, func(m *nats.Msg) {
		log.Info("Received stats of size: ", len(m.Data))
		s := &stats.Stats{}
		err := proto.Unmarshal(m.Data, s)
		if err != nil {
			log.Warn(" Unmarshalling error: ", err)
			//ns.wg.Done()
			return
		}

		log.Debug("Received statistics: ", s)
	})
	<-quit

	return nil
}

func (subscriber Sub) SubscriberTest(topic string) error {

	log.Info("<- Subscriber Mode ->")

	imsgRcvCount := 0
	//this map contains the statistics per each node as defined in the struct Statistics
	//the uint32 is the identifier for the publisher nodes
	statmap := make(map[uint32]msg.Statistics)
	quit := make(chan bool)
	go printTestStat(statmap)
	switch subscriber.Protocol {
	case protocol.NATS:
		subscriber.conn.Nats.Subscribe(topic, func(m *nats.Msg) {
			handleSubTest(subscriber, m.Data, imsgRcvCount, statmap)
		})
		<-quit
	case protocol.RabbitMQ:
		channel := subscriber.conn.Rabbitmq
		q, err := channel.QueueDeclare(
			"",    // name
			true,  // durable
			false, // delete when usused
			true,  // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			return errors.New("Unable to instantiate the queue")
		}
		err = channel.QueueBind(
			q.Name, // queue name
			"",     // routing key
			topic,  // exchange
			false,
			nil)
		if err != nil {
			return errors.New("Failed to bind a queue")
		}

		msgs, err := channel.Consume(
			q.Name, // queue
			"",     // consumer
			true,   // auto-ack
			false,  // exclusive
			false,  // no-local
			false,  // no-wait
			nil,    // args
		)
		if err != nil {
			return errors.New("Failed to bind a queue")
		}
		go func() {
			for m := range msgs {
				handleSubTest(subscriber, m.Body, imsgRcvCount, statmap)
			}
		}()
		<-quit
	case protocol.MQTT:
		if token := subscriber.conn.MQTTClient.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
			handleSubTest(subscriber, msg.Payload(), imsgRcvCount, statmap)

		}); token.Wait() && token.Error() != nil {
			log.Error("MQTT Error")
			return errors.New("Impossible to subscribe to the topic " + topic)
		}
		<-quit
	case protocol.Kafka:
	//consumer, err := sarama.NewConsumer([]string{"10.100.0.168:9092"}, config)
	//
	//partitionList, err := subscriber.conn.Kafka.Consumer.Partitions(topic)
	//if err != nil {
	//	return errors.New("Failed to find partitions")
	//}
	//
	////messages := make(chan *sarama.ConsumerMessage, 256)
	////create the map to store the statistics of the arrived messages
	//statmap := make(map[uint32]msg.Statistics)
	//quit := make(chan bool)
	//for _, partition := range partitionList {
	//	partitionConsumer, err := subscriber.conn.Kafka.Consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	//	if err != nil {
	//		log.Error("Error while reading the partition: %s", err)
	//	} else {
	//		go func(partitionConsumer sarama.PartitionConsumer) {
	//			for message := range partitionConsumer.Messages() {
	//				handleSubTest(subscriber, message.Value, imsgRcvCount, statmap)
	//			}
	//
	//		}(partitionConsumer)
	//	}
	//}
	//<-quit
	//log.Info("Closing connection...")
	//return nil
	default:
		return errors.New("Error: SubscriberTest Incorrect Protocol")
	}
	return nil
}

func handleSubTest(sub Sub, data []byte, imsgRcvCount int, statmap map[uint32]msg.Statistics) {
	//create the map to store the statistics of the arrived messages
	metaData := msg.ParseMetadata(data)
	delay := (time.Now().UnixNano() - metaData.Timestamp) / 1e6
	//if the clientID of the received message
	//is the same of the the local clientId do not increase the stat
	if metaData.ClientID != sub.SubID {
		imsgRcvCount++
		//delay for the message in millisecond
		statmap[metaData.ClientID] = msg.Statistics{
			ReceivedMsg:     int32(statmap[metaData.ClientID].ReceivedMsg + 1),
			CumulativeDelay: int64(statmap[metaData.ClientID].CumulativeDelay + delay),
		}
		log.Info(" Received message: clientId %d msgSize %d MsgId %d Total Received "+"messages %d. ReceivedDelay(ms) %d",
			metaData.ClientID, len(data), metaData.MsgId, imsgRcvCount, delay)
	}
}

// print the statistics of the test
func printTestStat(statmap map[uint32]msg.Statistics) {
	for {
		if len(statmap) != 0 {
			fmt.Printf("\n\n\n\n" + strings.Repeat("#", 80))
			fmt.Printf("\n\t\tSTAT SUMMARY\n\n")
			for clientId, clientStat := range statmap {
				fmt.Printf("ClientId: %d ReceveidMsg: %d CumulativeDelay: %d ms\n", clientId,
					clientStat.ReceivedMsg, clientStat.CumulativeDelay)
			}
			fmt.Printf("\n\n\n\n")
			fmt.Printf(strings.Repeat("#", 80) + "\n")
			time.Sleep(2000 * time.Millisecond)
		}
	}
}
