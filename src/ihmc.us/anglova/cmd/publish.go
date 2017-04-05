package cmd

import (
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"ihmc.us/anglova/pubsub"
)

func init() {
	RootCmd.AddCommand(publishCmd)
}

var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "Publish to a topic",
	Long:  "Publish to a specific topic through the underlyng message broker",
	Run: func(cmd *cobra.Command, args []string) {
		pub, err := pubsub.NewPub(cfg.Protocol, cfg.BrokerAddress, cfg.BrokerPort, cfg.Topic)
		if err != nil {
			log.Fatal("Unable to create pub ", err)
		}

		pub.PublishSequence(cfg.Topic, cfg.MessageNumber, cfg.MessageSize, cfg.PublishInterval)
	},
}
