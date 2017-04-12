package pubsub

import (
	"errors"
	_ "github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"
	"ihmc.us/anglova/conn"
	"ihmc.us/anglova/msg"
	"ihmc.us/anglova/protocol"
	"math/rand"
	"sync/atomic"
	"time"
	//"github.com/Shopify/sarama"
	"github.com/Shopify/sarama"
	"ihmc.us/anglova/conn"
	"ihmc.us/anglova/msg"
	"ihmc.us/anglova/protocol"
)

type Pub struct {
	Protocol string
	conn     conn.Conn
	ID       uint32
}

func NewPub(protocol string, host string, port string, topic string) (*Pub, error) {
	id := rand.Uint32() + uint32(time.Now().Nanosecond())

	//create the connection
	connection, err := conn.New(protocol, host, port, topic)
	if err != nil {
		log.Error("Error during the connection with the broker!")
		return nil, err
	}
	return &Pub{Protocol: protocol,
		ID:   id,
		conn: *connection}, nil
}

//implement the ping
func (pub *Pub) PublishSequence(topic string, messageNumber int, messageSize int, publishInterval time.Duration) error {

	log.Info("PublishSequence ->")

	var msgSentCount uint32 = 0
	for i := 0; i < messageNumber; i++ {
		msg, err := msg.New(pub.ID, msgSentCount, time.Now().UnixNano(), messageSize)
		if err != nil {
			errors.New("Unable to create msg")
			return err
		}

		err = pub.Publish(topic, msg.Bytes())

		if err != nil {
			log.Error("Unable to push the msg", err)
		}
		atomic.AddUint32(&msgSentCount, 1)
		log.Info("Sent msg: msgId %d", atomic.LoadUint32(&msgSentCount))
		time.Sleep(publishInterval)
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
		_, err = pub.conn.ZMQClient.Pub.Send(topic+" "+string(buf[:]), 0)
	case protocol.Redis:
		pub.conn.RedisClient.Lock()
		pub.conn.RedisClient.Conn.Send("PUBLISH", topic, buf)
		pub.conn.RedisClient.Unlock()
	default:
		return errors.New("Unsupported protocol")
	}

	return err
}
