package cmd

import (
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"ihmc.us/anglova/sub"
	"ihmc.us/anglova/protocol"
)

func init() {
	RootCmd.AddCommand(statsCmd)
}

var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "Operate as a stats server",
	Long:  "Operate as a stats server and aggregate statistics sent from subscribers",
	Run: func(cmd *cobra.Command, args []string) {
		var subscriber *sub.Sub
		var err error

		//use NATS by default to aggregate
		subscriber, err = sub.New(protocol.NATS, cfg.StatsAddress, cfg.Port, "stats")
		if err != nil {
			log.Fatal("Unable to create subscriber", err)
		}

		//launch the test
		subscriber.CollectStats("stats")
	},
}
