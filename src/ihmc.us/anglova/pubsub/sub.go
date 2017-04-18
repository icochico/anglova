package pubsub

import (
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	log "github.com/Sirupsen/logrus"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/protobuf/proto"
	"github.com/nats-io/go-nats"
	"ihmc.us/anglova/conn"
	"ihmc.us/anglova/msg"
	"ihmc.us/anglova/protocol"
	"ihmc.us/anglova/stats"
	"math/rand"
	"strings"
	"time"
	"sync"
	"os"
	"encoding/csv"
	"strconv"
)

const StatsTopic = "stats"

type Sub struct {
	Protocol  string
	conn      conn.Conn
	statsConn conn.Conn
	ID        int32
	Topic     string
}

//lock to sync the reading and writing operations on the below maps
var lock = sync.RWMutex{}

//PubID MsgId MsgSize sentTime
var sentTimes = make(map[int32]map[int32]map[int32]time.Time)
//PubId MsgID MsgSize SubID RcvTime
var rcvTimes = make(map[int32]map[int32]map[int32]map[int32]time.Time)

func NewSub(proto string, host string, port string, topic string, statsHost string, statsPort string) (*Sub, error) {
	id, err := CreateNodeID()
	if err != nil {
		log.Error(err)
		id = rand.Int31()
	}
	connection, err := conn.New(proto, host, port, topic, false)
	if err != nil {
		log.Error("Error during the connection with the broker!")
		return nil, err
	}
	sConn, err := conn.New(protocol.NATS, statsHost, statsPort, StatsTopic, true)
	if err != nil {
		log.Error("Error during the connection with the stats broker!")
		return nil, err
	}
	return &Sub{Protocol: proto,
		conn:         *connection,
		statsConn:    *sConn,
		ID:           id,
		Topic:        topic}, nil
}

//stats server function
//the stat server subscribe to the sub topic to obtain the message info
//it will use the info to calculate the delayed time
func (sub Sub) SubscribeToStats() error {

	if sub.Protocol != protocol.NATS {
		return errors.New("SubscribeToStats only available with NATS Broker")
	}
	quit := make(chan bool)
	go handleStatInfo(&sub, quit)
	go handleStatGen(quit)
	<-quit

	return nil
}

//handle the StatsSenderTopic info
func handleStatInfo(sub *Sub, quit chan bool) error {
	sub.statsConn.NATSClient.Subscribe(StatsTopic, func(m *nats.Msg) {
		log.Info("Received stats of size: ", len(m.Data))
		s := &stats.Stats{}
		err := proto.Unmarshal(m.Data, s)
		if err != nil {
			log.Warn(" Unmarshalling error: ", err)
			return
		}
		lock.Lock()
		if s.ClientType == stats.ClientType_Publisher {
			log.Debug("Received statistics from publisher: ", s.PublisherId, s.SubscriberId, s.MessageId)
			if len(sentTimes[s.PublisherId]) == 0 {
				sentTimes[s.PublisherId] = make(map[int32]map[int32]time.Time)
			}
			if len(sentTimes[s.PublisherId][s.MessageId]) == 0 {
				sentTimes[s.PublisherId][s.MessageId] = make(map[int32]time.Time)
			}
			sentTimes[s.PublisherId][s.MessageId][s.MessageSize] = time.Now()
		} else {
			fmt.Println("Received statistics from subscriber: ", s.PublisherId, s.SubscriberId, s.MessageId)
			if len(rcvTimes[s.PublisherId]) == 0 {
				rcvTimes[s.PublisherId] = make(map[int32]map[int32]map[int32]time.Time)
			}
			if len(rcvTimes[s.PublisherId][s.MessageId]) == 0 {
				rcvTimes[s.PublisherId][s.MessageId] = make(map[int32]map[int32]time.Time)
			}
			if len(rcvTimes[s.PublisherId][s.MessageId][s.MessageSize]) == 0 {
				rcvTimes[s.PublisherId][s.MessageId][s.MessageSize] = make(map[int32]time.Time)
			}
			rcvTimes[s.PublisherId][s.MessageId][s.MessageSize][s.SubscriberId] = time.Now()
		}
		lock.Unlock()
	})
	<-quit
	return nil
}

func handleStatGen(quit chan bool) {
	//print the result on a CSV file periodically (60 seconds)
	for {
		file, err := os.Create("TimeResults.csv" )
		if err != nil {
			log.Error("Impossible to create the csv File")
		}
		writer := csv.NewWriter(file)
		lock.RLock()
		for pubID, msgs := range rcvTimes {
			//log.Info("Pubs: ", msgs)
			fmt.Println("\n\nSUMMARY")
			for msgID, msgSizes := range msgs {
				var totalDelayForMessage int64
				var reachedSubscribers int32
				for size, subs := range msgSizes {
					for _, rTime := range subs {
						totalDelayForMessage += (rTime.UnixNano() - sentTimes[pubID][msgID][size].UnixNano()) / 1e6
						reachedSubscribers++
					}
					fmt.Println("PubId", pubID, "MsgId ", msgID, "MsgSize", size, "TotalDelayForMsg ", totalDelayForMessage, "ReachedSubs ", reachedSubscribers)
					res := []string{strconv.FormatInt(int64(pubID), 10), strconv.FormatInt(int64(msgID), 10), strconv.FormatInt(int64(size), 10),
							strconv.FormatInt(int64(totalDelayForMessage), 10), strconv.FormatInt(int64(reachedSubscribers), 10)}
					//log.Info("results: ", res)
					err := writer.Write(res)
					if err != nil {
						log.Error("Impossible to write on the CSV file")
					}
					writer.Flush()
				}
			}
			fmt.Println("\n\nEND SUMMARY\n\n")
		}
		file.Close()
		lock.RUnlock()
		time.Sleep(10 * time.Second)
	}
	<-quit
}
func (sub Sub) Subscribe(topic string) error {

	log.Info("Subscribe <-")

	imsgRcvCount := 0
	//this map contains the statistics per each node as defined in the struct Statistics
	//the uint32 is the identifier for the pub nodes
	statmap := make(map[int32]msg.Statistics)
	quit := make(chan bool)
	go printTestStat(statmap)
	switch sub.Protocol {
	case protocol.NATS:
		sub.conn.NATSClient.Subscribe(topic, func(m *nats.Msg) {
			handleSubTest(sub, m.Data, imsgRcvCount, statmap)
		})
		<-quit
	case protocol.RabbitMQ:
		channel := sub.conn.RabbitMQClient
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
				handleSubTest(sub, m.Body, imsgRcvCount, statmap)
			}
		}()
		<-quit
	case protocol.MQTT:
		if token := sub.conn.MQTTClient.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
			handleSubTest(sub, msg.Payload(), imsgRcvCount, statmap)

		}); token.Wait() && token.Error() != nil {
			log.Error("MQTT Error")
			return errors.New("Impossible to subscribe to the topic " + topic)
		}
		<-quit
	case protocol.Kafka:
		//consumer, err := sarama.NewConsumer([]string{"10.100.0.168:9092"}, config)

		partitionList, err := sub.conn.KafkaClient.Consumer.Partitions(topic)
		if err != nil {
			return errors.New("Failed to find partitions")
		}

		//messages := make(chan *sarama.ConsumerMessage, 256)
		//create the map to store the statistics of the arrived messages
		statmap := make(map[int32]msg.Statistics)
		quit := make(chan bool)
		for _, partition := range partitionList {
			partitionConsumer, err := sub.conn.KafkaClient.Consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
			if err != nil {
				log.Error("Error while reading the partition: %s", err)
			} else {
				go func(partitionConsumer sarama.PartitionConsumer) {
					for message := range partitionConsumer.Messages() {
						handleSubTest(sub, message.Value, imsgRcvCount, statmap)
					}

				}(partitionConsumer)
			}
		}
		<-quit
		log.Info("Closing connection...")
		return nil
	case protocol.IPFS:
		subscription, err := sub.conn.IPFSClient.PubSubSubscribe(topic)
		if err != nil {
			return err
		}
		for record, err := subscription.Next(); err != nil; {
			handleSubTest(sub, record.Data(), imsgRcvCount, statmap)
		}
		<-quit
	case protocol.ZMQ:
		err := sub.conn.ZMQClient.SetSubscribe(topic)
		if err != nil {
			return err
		}
		for {
			message, err := sub.conn.ZMQClient.RecvMessageBytes(0)
			if err != nil {
				log.Fatal("Error receiving messages", err)
				break
			}
			handleSubTest(sub, message[1], imsgRcvCount, statmap)
		}
		<-quit
	case protocol.Redis:
		sub.conn.RedisClient.Subscribe(topic)
		for {
			switch v := sub.conn.RedisClient.Receive().(type) {
			case redis.Message:
				handleSubTest(sub, v.Data, imsgRcvCount, statmap)
			}
		}
		<-quit
	default:
		return errors.New("Error: Subscribe Incorrect Protocol")
	}
	return nil
}

func handleSubTest(sub Sub, data []byte, imsgRcvCount int, statmap map[int32]msg.Statistics) {
	//create the map to store the statistics of the arrived messages
	metaData := msg.ParseMetadata(data)
	delay := (time.Now().UnixNano() - metaData.Timestamp) / 1e6
	//if the clientID of the received message
	//is the same of the the local clientId do not increase the stat
	if metaData.ClientID != sub.ID {
		//is the msg out of order?
		outOfOrder := false
		if statmap[metaData.ClientID].ReceivedMsg != metaData.MsgId {
			log.Debug("Out of Order? ", imsgRcvCount, metaData.MsgId)
			outOfOrder = true
		}
		imsgRcvCount++
		//delay for the message in millisecond
		mstat := msg.Statistics{
			ReceivedMsg:     int32(statmap[metaData.ClientID].ReceivedMsg + 1),
			CumulativeDelay: int64(statmap[metaData.ClientID].CumulativeDelay + delay),
		}
		if outOfOrder {
			mstat.OutOfOrderMsgs++
		}
		statmap[metaData.ClientID] = mstat
		log.Info(" Received message: clientId ", metaData.ClientID," msgSize ",  len(data), " MsgId ",  metaData.MsgId,
			"Total Received messages", imsgRcvCount, " ReceivedDelay(ms) ", delay, "OutofOrder: ", outOfOrder)
		//send the msg info to the stats server
		stat := &stats.Stats{}
		stat.ClientType = stats.ClientType_Subscriber
		stat.PublisherId = metaData.ClientID
		stat.SubscriberId = sub.ID
		stat.MessageId = metaData.MsgId
		buf, err := proto.Marshal(stat)
		if err != nil {
			log.Error("Impossible to Marshal the stat message")
		}
		//fmt.Println("about to send statistics ", stat.PublisherId, stat.SubscriberId, stat.MessageId )
		err = sub.statsConn.NATSClient.Publish(StatsTopic, buf)
		if err != nil {
			log.Error("Impossible to send the msg Info to the stats broker")
		}
	}
}

// print the statistics of the test on the console
// also write the result on a CSV file
// the correct cumulative delay may be found on the stat server
func printTestStat(statmap map[int32]msg.Statistics) {
	for {
		resFile, err := os.Create("nodeStats.csv")
		writer := csv.NewWriter(resFile)
		if err != nil {
			log.Error("Error in creating the csv file for the node stat results")
			//terminate this gorutine
			return
		}
		if len(statmap) != 0 {
			fmt.Printf("\n\n\n\n" + strings.Repeat("#", 80))
			fmt.Printf("\n\t\tSTAT SUMMARY\n\n")
			for clientId, clientStat := range statmap {
				fmt.Printf("ClientId: %d  ReceveidMsg: %d CumulativeDelay: %d ms  OutOfOrderMsgs: %d\n",clientId,
					 clientStat.ReceivedMsg, clientStat.CumulativeDelay, clientStat.OutOfOrderMsgs)
				res := []string{strconv.FormatInt(int64(clientId), 10), strconv.FormatInt(int64(clientStat.ReceivedMsg), 10),
						strconv.FormatInt(int64(clientStat.CumulativeDelay), 10), strconv.FormatInt(int64(clientStat.OutOfOrderMsgs), 10)}
				writer.Write(res)
				writer.Flush()
			}
			resFile.Close()
			fmt.Printf("\n\n\n\n")
			fmt.Printf(strings.Repeat("#", 80) + "\n")
			time.Sleep(2000 * time.Millisecond)
		}
	}
}
