package cmd

import (
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"ihmc.us/anglova/protocol"
	"ihmc.us/anglova/pubsub"
)

func init() {
	RootCmd.AddCommand(statsCmd)
}

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Run the stats server",
	Long:  "Run the stats server and aggregate statistics sent from publishers and subscribers",
	Run: func(cmd *cobra.Command, args []string) {
		var subscriber *pubsub.Sub
		var err error

		//use NATS by default to aggregate
		subscriber, err = pubsub.NewSub(protocol.NATS, cfg.StatsAddress, cfg.StatsPort, "", cfg.StatsAddress, cfg.StatsPort)
		if err != nil {
			log.Fatal("Unable to create subscriber", err)
		}

		//launch the test
		subscriber.SubscribeToStats()
	},
}
