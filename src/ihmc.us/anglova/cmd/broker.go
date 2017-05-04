package cmd
/*
import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	zmq "github.com/pebbe/zmq4"
	"github.com/spf13/cobra"
	"strconv"
	"time"
)

func init() {
	RootCmd.AddCommand(brokerCmd)
}

var brokerCmd = &cobra.Command{
	Use:   "broker",
	Short: "Start a ZMQ broker",
	Long:  "Starts a ZMQ broker than can be used to publish and subscribe to",
	Run: func(cmd *cobra.Command, args []string) {
		var publishPort int
		var publishHost string
		var receiverHost string
		var err error

		receiverHost = fmt.Sprintf("tcp://*:%s", cfg.BrokerPort)

		if publishPort, err = strconv.Atoi(cfg.BrokerPort); err == nil {
			publishHost = fmt.Sprintf("tcp://*:%d", publishPort+1)
		} else {
			log.Fatal("Unable to create ZMQ broker ", err)
		}

		context, _ := zmq.NewContext()
		receiver, _ := context.NewSocket(zmq.PULL)
		receiver.Bind(receiverHost)
		log.Info("Bound receiver on " + receiverHost)
		sender, _ := context.NewSocket(zmq.PUB)
		sender.Bind(publishHost)
		log.Info("Bound publisher on " + publishHost)

		log.Info("Waiting for messages...")

		last := time.Now()
		messages := 0
		quiet := true
		for {
			message, err := receiver.RecvMessageBytes(0)
			if err != nil {
				log.Error("Error receiving messages", err)
			}
			sender.SendMessage(message)
			if !quiet {
				messages += 1
				now := time.Now()
				if now.Sub(last).Seconds() > 1 {
					log.Info(messages, "msg/sec")
					last = now
					messages = 0
				}
			}

		}
	},
}
*/