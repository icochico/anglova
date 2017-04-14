package pubsub

import (
	"errors"
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/gogo/protobuf/proto"
	"github.com/streadway/amqp"
	"ihmc.us/anglova/conn"
	"ihmc.us/anglova/msg"
	"ihmc.us/anglova/protocol"
	"ihmc.us/anglova/stats"
	"math/rand"
	"sync/atomic"
	"time"
	"net"
	"strings"
	"strconv"
)

type Pub struct {
	Protocol  string
	conn      conn.Conn
	statsConn conn.Conn
	ID        int32
}

//this function returns the last three digits of the IP address
//to create the nodeID. For the testbed configuration the 10.... class address will be used
func CreateNodeID() (int32, error){
	ifaces, err := net.Interfaces()
	if err != nil {
		log.Error("Impossible to get the Interfaces")
		return 0, err
	}
	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			log.Error("Error in getting the interface addresses")
			return nil, err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				continue
			case *net.IPAddr:
				ip = v.IP
			}
			if ip.To4() != nil {
				var ipString string
				ipString = ip.To4().String()
				ipFour := strings.Split(ipString, ".")
				if ipFour[0] == "10" {
					nodeID, err := strconv.Atoi(ipFour[3])
					if err != nil {
						log.Error("Error in converting the IP string into an int")
						return 0, err
					}
					return int32(nodeID), nil
				}
			}
		}
	}
	return 0, errors.New("Impossible to create a nodeID")
}

func NewPub(proto string, host string, port string, topic string, statsAddress string, statsPort string) (*Pub, error) {
	id, err := CreateNodeID()
	if err != nil {
		log.Error(err)
		id = rand.Int31()
	}
	//create the connection
	connection, err := conn.New(proto, host, port, topic, true)
	if err != nil {
		log.Error("Error during the connection with the broker!")
		return nil, err
	}
	//create the stats connection
	sConnection, err := conn.New(protocol.NATS, statsAddress, statsPort, StatsTopic, true)
	if err != nil {
		log.Error("Error during the connection with the Stats broker!")
		return nil, err
	}
	return &Pub{Protocol: proto,
		ID:        id,
		conn:      *connection,
		statsConn: *sConnection}, nil
}

//implement the ping
func (pub *Pub) PublishSequence(topic string, messageNumber int, messageSize int, publishInterval time.Duration) error {

	log.Info("PublishSequence ->")

	var msgSentCount int32 = 0
	for i := 0; i < messageNumber; i++ {
		msg, err := msg.New(pub.ID, msgSentCount, time.Now().UnixNano(), messageSize)
		if err != nil {
			errors.New("Unable to create msg")
			return err
		}

		err = pub.Publish(topic, msg.Bytes())
		pub.PublishStats(msgSentCount, int32(messageSize))
		if err != nil {
			log.Error("Unable to push the msg", err)
		}
		atomic.AddInt32(&msgSentCount, 1)
		log.Info("Sent msg: msgId ", atomic.LoadInt32(&msgSentCount))
		time.Sleep(publishInterval)
	}

	// Make sure that Redis flushes its buffer after sending the messages
	if pub.Protocol == protocol.Redis {
		pub.conn.RedisClient.Lock()
		pub.conn.RedisClient.Conn.Flush()
		pub.conn.RedisClient.Unlock()
	}

	return nil
}

func (pub *Pub) Publish(topic string, buf []byte) error {
	var err error

	switch pub.Protocol {
	case protocol.NATS:
		err = pub.conn.NATSClient.Publish(topic, buf)
	case protocol.RabbitMQ:
		channel := pub.conn.RabbitMQClient
		err = channel.Publish(
			topic, // exchange
			"",    // routing key
			true,  // mandatory
			false, // immediate
			amqp.Publishing{
				Body: buf,
			})
	case protocol.MQTT:
		//consider if we have to implement the possibility to retain a message
		//and if we have to check the ack from the broker
		token := pub.conn.MQTTClient.Publish(topic, 0, false, buf)
		token.Wait()
	case protocol.Kafka:
		kafkaMessage := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(buf)}
		_, _, err = pub.conn.KafkaClient.Producer.SendMessage(kafkaMessage)
	case protocol.IPFS:
		err = pub.conn.IPFSClient.PubSubPublish(topic, string(buf[:]))
	case protocol.ZMQ:
		_, err = pub.conn.ZMQClient.SendMessage(topic, buf)
	case protocol.Redis:
		pub.conn.RedisClient.Lock()
		pub.conn.RedisClient.Conn.Send("PUBLISH", topic, buf)
		pub.conn.RedisClient.Unlock()
	default:
		return errors.New("Unsupported protocol")
	}

	return err
}

func (pub *Pub) PublishStats(msgCount int32, msgSize int32) {
	stat := &stats.Stats{}
	stat.ClientType = stats.ClientType_Publisher
	stat.PublisherId = pub.ID
	//log.Info("PublisherID: ", pub.ID)
	stat.MessageId = msgCount
	stat.MessageSize = msgSize
	//log.Info("Publisher: about to send statistics", stat.PublisherId, stat.SubscriberId)
	statBuf, err := proto.Marshal(stat)
	if err != nil {
		log.Error(err)
	}
	err = pub.statsConn.NATSClient.Publish(StatsTopic, statBuf)
	if err != nil {
		log.Error("Error sending stats to the HQ")
	}
}
