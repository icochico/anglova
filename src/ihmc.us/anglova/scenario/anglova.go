package scenario

import (
	"time"
	"ihmc.us/anglova/pubsub"
	log "github.com/Sirupsen/logrus"
	"ihmc.us/anglova/config"
	"ihmc.us/anglova/msg"
	"ihmc.us/anglova/protocol"
	"ihmc.us/anglova/stats"
	"github.com/gogo/protobuf/proto"
)

type Anglova struct {
	Cfg  config.Manager
	Quit chan bool
}

// sizes in bytes, intervals in seconds
const (
	BlueForceTopic         = "blueforce"
	BlueForceTrackSize     = 512
	BlueForceTrackInterval = 5
	SensorDataTopic        = "sensordata"
	SensorDataSize         = 128000
	SensorDataInterval     = 300 // 5 minutes
	HQReportTopic          = "hqreport"
	HQReportSize           = 1000000
	HQReportInterval       = 600 // 10 minutes
	StatsTopic = "stats"
)

func NewAnglova(cfg config.Manager) (*Anglova) {

	return &Anglova{Cfg: cfg,
		Quit:        make(chan bool)}
}

func (a *Anglova) Run() {

	var quitChannels []chan bool

	//for statistics, use nats
	pubStats, err := pubsub.NewPub(protocol.NATS, a.Cfg.StatsAddress, a.Cfg.StatsPort, StatsTopic)
	if err != nil {
		log.Error(err)
	}
	//TODO verify if using one shared publisher for stats is goroutine safe

	if a.Cfg.EnableBlueForce {
		quitBF := make(chan bool)
		quitChannels = append(quitChannels, quitBF)
		pubBF, err := pubsub.NewPub(a.Cfg.Protocol, a.Cfg.BrokerAddress, a.Cfg.BrokerPort, BlueForceTopic)
		if err != nil {
			log.Error(err)
		}
		blueForcePublishRoutine(pubBF, pubStats, quitBF)
	}

	if a.Cfg.EnableSensorData {
		quitSD := make(chan bool)
		quitChannels = append(quitChannels, quitSD)
		pubSD, err := pubsub.NewPub(a.Cfg.Protocol, a.Cfg.BrokerAddress, a.Cfg.BrokerPort, SensorDataTopic)
		if err != nil {
			log.Error(err)
		}
		sensorDataPublishRoutine(pubSD, pubStats, quitSD)
	}


	if a.Cfg.EnableHQReport {
		quitHQ := make(chan bool)
		quitChannels = append(quitChannels, quitHQ)
		pubHQ, err := pubsub.NewPub(a.Cfg.Protocol, a.Cfg.BrokerAddress, a.Cfg.BrokerPort, HQReportTopic)
		if err != nil {
			log.Error(err)
		}
		hqReportPublishRoutine(pubHQ, pubStats, quitHQ)

	}

	// wait for the scenario to quit, quit go routines
	select {
	case <-a.Quit:
		{
			for _, quit := range quitChannels {
				quit <- true
			}
		}
	}

}

func blueForcePublishRoutine(pub *pubsub.Pub, pubStats *pubsub.Pub, quit chan bool) {
	ticker := time.NewTicker(time.Second * BlueForceTrackInterval)
	var msgCount uint32
	go func() {
		for {
			select {
			case <-ticker.C:
				message, err := msg.New(pub.ID, msgCount, time.Now().UnixNano(), BlueForceTrackSize)
				if err != nil {
					log.Error(err)
				}
				pub.Publish(BlueForceTopic, message.Bytes())
				publishStats(pubStats, msgCount)
				msgCount++
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func sensorDataPublishRoutine(pub *pubsub.Pub, pubStats *pubsub.Pub, quit chan bool) {
	ticker := time.NewTicker(time.Second * SensorDataInterval)
	var msgCount uint32
	go func() {
		for {
			select {
			case <-ticker.C:
				message, err := msg.New(pub.ID, msgCount, time.Now().UnixNano(), SensorDataSize)
				if err != nil {
					log.Error(err)
				}
				pub.Publish(SensorDataTopic, message.Bytes())
				publishStats(pubStats, msgCount)
				msgCount++
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func hqReportPublishRoutine(pub *pubsub.Pub, pubStats *pubsub.Pub, quit chan bool) {

	ticker := time.NewTicker(time.Second * HQReportInterval)
	var msgCount uint32
	go func() {
		for {
			select {
			case <-ticker.C:
				message, err := msg.New(pub.ID, msgCount, time.Now().UnixNano(), HQReportSize)
				if err != nil {
					log.Error(err)
				}
				pub.Publish(HQReportTopic, message.Bytes())
				publishStats(pubStats, msgCount)
				msgCount++
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func publishStats(pub *pubsub.Pub, msgCount uint32) {
	stat := &stats.Stats{}
	stat.ClientType = stats.ClientType_Publisher
	stat.PublisherId = int32(pub.ID)
	stat.MessageId = int32(msgCount)
	statBuf, err := proto.Marshal(stat)
	if err != nil {
		log.Error(err)
	}
	pub.Publish(StatsTopic, statBuf)
}
