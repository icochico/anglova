package cmd

import (
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"ihmc.us/anglova/pub"
	"ihmc.us/anglova/sub"
)

func init() {
	RootCmd.AddCommand(pubSubCmd)
}

var pubSubCmd = &cobra.Command{
	Use:   "pubsub",
	Short: "Publish and Subscribe to a topic",
	Long:  "Subscribe to a specific topic and publish through the underlyng message broker",
	Run: func(cmd *cobra.Command, args []string) {
		publisher, err := pub.New(cfg.Protocol, cfg.BrokerAddress, cfg.Port, cfg.Topic)
		if err != nil {
			log.Fatal("Unable to create publisher ", err)
		}
		subscriber, err := sub.New(cfg.Protocol, cfg.BrokerAddress, cfg.Port, cfg.Topic)
		if err != nil {
			log.Fatal("Unable to create subscriber", err)
		}

		//use a little hack to setup the test
		//the publisher and the subscriber in this configuration are obviously the same
		//the subscriber does not want to receiver the messages it has submitted
		//the ID of the two entities must be the same
		subscriber.SubID = publisher.PubID
		//launch both the publisher and the subscriber
		//create a channel to wait the termination of the two goroutines
		endTest := make(chan bool)
		go subscriber.SubscriberTest(cfg.Topic)
		go publisher.PublishTest(cfg.Topic, cfg.MessageNumber, cfg.MessageSize, cfg.PublishInterval)
		<-endTest
	},
}
