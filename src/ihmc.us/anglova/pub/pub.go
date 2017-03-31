package pub

import (
	"errors"
	log "github.com/Sirupsen/logrus"
	"math/rand"
	"time"
	"sync/atomic"
	"github.com/streadway/amqp"
	_"github.com/Shopify/sarama"
	//MQTT "github.com/eclipse/paho.mqtt.golang"
	"ihmc.us/anglova/protocol"
	"ihmc.us/anglova/conn"
	"ihmc.us/anglova/msg"
)

type Pub struct {
	Protocol string
	conn     conn.Conn
	PubID    uint32
}

func New(protocol string, host string, port string, topic string) (*Pub, error) {
	pubId := rand.Uint32() + uint32(time.Now().Nanosecond())

	//create the connection
	connection, err := conn.New(protocol, host, port, topic)
	if err != nil {
		log.Error("Error during the connection with the broker!")
		return nil, err
	}
	return &Pub{Protocol: protocol,
		PubID:        pubId,
		conn:         *connection}, nil
}

//implement the ping
func (publisher *Pub) PublishTest(topic string, messageNumber int, messageSize int, publishInterval time.Duration) error {

	log.Info("<- Publisher Mode ->")

	var imsgSentCount uint32 = 0
	for i := 0; i < messageNumber; i++ {
		message, err := msg.New(publisher.PubID, imsgSentCount, time.Now().UnixNano(), messageSize)
		if err != nil {
			errors.New("Impossible to create the message")
			return err
		}
		switch publisher.Protocol {
		case protocol.NATS:
			err = publisher.conn.Nats.Publish(topic, message.Bytes())
		case protocol.RabbitMQ:
			channel := publisher.conn.Rabbitmq
			err = channel.Publish(
				topic, // exchange
				"",    // routing key
				true,  // mandatory
				false, // immediate
				amqp.Publishing{
					Body: message.Bytes(),
				})
		case protocol.MQTT:
			//consider if we have to implement the possibility to retain a message
			//and if we have to check the ack from the broker
			token := publisher.conn.MQTTClient.Publish(topic, 0, false, message.Bytes())
			token.Wait()
		case protocol.Kafka:
		//kafkaMessage := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(message.Bytes()), }
		//_, _, err = publisher.conn.Kafka.Producer.SendMessage(kafkaMessage)
		default:
			return errors.New("No correct protocol specified")
		}
		if err != nil {
			log.Error("Unable to push the message", err)
		}
		atomic.AddUint32(&imsgSentCount, 1)
		log.Info("Sent message: msgId %d", atomic.LoadUint32(&imsgSentCount))
		time.Sleep(publishInterval)
	}
	return nil
}
