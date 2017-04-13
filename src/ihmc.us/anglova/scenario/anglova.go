package scenario

import (
	"time"
	"ihmc.us/anglova/pubsub"
	log "github.com/Sirupsen/logrus"
	"ihmc.us/anglova/config"
	"ihmc.us/anglova/msg"
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
)

func NewAnglova(cfg config.Manager) (*Anglova) {

	return &Anglova{Cfg: cfg,
		Quit:        make(chan bool)}
}

func (a *Anglova) Run() {

	var quitChannels []chan bool

	//TODO verify if using one shared publisher for stats is goroutine safe

	if a.Cfg.EnableBlueForce {
		quitBF := make(chan bool)
		quitChannels = append(quitChannels, quitBF)
		pubBF, err := pubsub.NewPub(a.Cfg.Protocol, a.Cfg.BrokerAddress, a.Cfg.BrokerPort, BlueForceTopic, a.Cfg.StatsAddress, a.Cfg.StatsPort)
		if err != nil {
			log.Error(err)
		}
		blueForcePublishRoutine(pubBF, quitBF)
	}

	if a.Cfg.EnableSensorData {
		quitSD := make(chan bool)
		quitChannels = append(quitChannels, quitSD)
		pubSD,err  := pubsub.NewPub(a.Cfg.Protocol, a.Cfg.BrokerAddress, a.Cfg.BrokerPort, SensorDataTopic, a.Cfg.StatsAddress, a.Cfg.StatsPort)
		if err != nil {
			log.Error(err)
		}
		sensorDataPublishRoutine(pubSD, quitSD)
	}

	if a.Cfg.EnableHQReport {
		quitHQ := make(chan bool)
		quitChannels = append(quitChannels, quitHQ)
		pubHQ, err := pubsub.NewPub(a.Cfg.Protocol, a.Cfg.BrokerAddress, a.Cfg.BrokerPort, HQReportTopic, a.Cfg.StatsAddress, a.Cfg.StatsPort)
		if err != nil {
			log.Error(err)
		}
		hqReportPublishRoutine(pubHQ, quitHQ)

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

func blueForcePublishRoutine(pub *pubsub.Pub, quit chan bool) {
	ticker := time.NewTicker(time.Second * BlueForceTrackInterval)
	var msgCount int32
	go func() {
		for {
			select {
			case <-ticker.C:
				message, err := msg.New(pub.ID, msgCount, time.Now().UnixNano(), BlueForceTrackSize)
				if err != nil {
					log.Error(err)
				}
				pub.Publish(BlueForceTopic, message.Bytes())
				pub.PublishStats(msgCount)
				msgCount++
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func sensorDataPublishRoutine(pub *pubsub.Pub, quit chan bool) {
	ticker := time.NewTicker(time.Second * SensorDataInterval)
	var msgCount int32
	go func() {
		for {
			select {
			case <-ticker.C:
				message, err := msg.New(pub.ID, msgCount, time.Now().UnixNano(), SensorDataSize)
				if err != nil {
					log.Error(err)
				}
				pub.Publish(SensorDataTopic, message.Bytes())
				pub.PublishStats(msgCount)
				msgCount++
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}

func hqReportPublishRoutine(pub *pubsub.Pub, quit chan bool) {

	ticker := time.NewTicker(time.Second * HQReportInterval)
	var msgCount int32
	go func() {
		for {
			select {
			case <-ticker.C:
				message, err := msg.New(pub.ID, msgCount, time.Now().UnixNano(), HQReportSize)
				if err != nil {
					log.Error(err)
				}
				pub.Publish(HQReportTopic, message.Bytes())
				pub.PublishStats(msgCount)
				msgCount++
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
}
