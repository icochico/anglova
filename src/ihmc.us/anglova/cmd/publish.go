package cmd

import (
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"ihmc.us/anglova/pub"
)

func init() {
	RootCmd.AddCommand(publishCmd)
}

var publishCmd = &cobra.Command{
	Use:   "publish",
	Short: "Publish to a topic",
	Long:  "Publish to a specific topic through the underlyng message broker",
	Run: func(cmd *cobra.Command, args []string) {
		publisher, err := pub.New(cfg.Protocol, cfg.BrokerAddress, cfg.Port, cfg.Topic)
		if err != nil {
			log.Fatal("Unable to create publisher ", err)
		}

		publisher.PublishTest(cfg.Topic, cfg.MessageNumber, cfg.MessageSize, cfg.PublishInterval)
	},
}
