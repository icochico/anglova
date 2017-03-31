package cmd

import (
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"ihmc.us/anglova/sub"
)

func init() {
	RootCmd.AddCommand(subscribeCmd)
}

var subscribeCmd = &cobra.Command{
	Use:   "subscribe",
	Short: "Subscribe to a topic",
	Long:  "Suscribe to a specific topic through the underlyng message broker",
	Run: func(cmd *cobra.Command, args []string) {
		var subscriber *sub.Sub
		var err error

		subscriber, err = sub.New(cfg.Protocol, cfg.BrokerAddress, cfg.Port, cfg.Topic)
		if err != nil {
			log.Fatal("Unable to create subscriber", err)
		}

		//launch the test
		subscriber.SubscriberTest(cfg.Topic)
	},
}
