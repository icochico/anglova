package cmd

import (
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"ihmc.us/anglova/pubsub"
)

func init() {
	RootCmd.AddCommand(pubSubCmd)
}

var pubSubCmd = &cobra.Command{
	Use:   "pubsub",
	Short: "Publish and subscribe to a topic",
	Long:  "Subscribe to a specific topic and publish through the underlyng message broker",
	Run: func(cmd *cobra.Command, args []string) {
		pub, err := pubsub.NewPub(cfg.Protocol, cfg.BrokerAddress, cfg.BrokerPort, cfg.Topic)
		if err != nil {
			log.Fatal("Unable to create pub ", err)
		}
		sub, err := pubsub.NewSub(cfg.Protocol, cfg.BrokerAddress, cfg.BrokerPort, cfg.Topic)
		if err != nil {
			log.Fatal("Unable to create sub", err)
		}

		//use a little hack to setup the test
		//the pub and the sub in this configuration are obviously the same
		//the sub does not want to receiver the messages it has submitted
		//the ID of the two entities must be the same
		sub.ID = pub.ID
		//launch both the pub and the sub
		//create a channel to wait the termination of the two goroutines
		endTest := make(chan bool)
		go sub.Subscribe(cfg.Topic)
		go pub.PublishSequence(cfg.Topic, cfg.MessageNumber, cfg.MessageSize, cfg.PublishInterval)
		<-endTest
	},
}
