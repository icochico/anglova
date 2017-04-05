package cmd

import (
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"ihmc.us/anglova/pubsub"
)

func init() {
	RootCmd.AddCommand(subscribeCmd)
}

var subscribeCmd = &cobra.Command{
	Use:   "subscribe",
	Short: "Subscribe to a topic",
	Long:  "Suscribe to a specific topic through the underlyng message broker",
	Run: func(cmd *cobra.Command, args []string) {
		var sub *pubsub.Sub
		var err error

		sub, err = pubsub.NewSub(cfg.Protocol, cfg.BrokerAddress, cfg.BrokerPort, cfg.Topic)
		if err != nil {
			log.Fatal("Unable to create sub", err)
		}

		//launch the test
		sub.Subscribe(cfg.Topic)
	},
}
