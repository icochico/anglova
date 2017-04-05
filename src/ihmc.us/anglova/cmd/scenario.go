package cmd

import (
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"ihmc.us/anglova/scenario"
)

func init() {
	RootCmd.AddCommand(scenarioCmd)
}

var scenarioCmd = &cobra.Command{
	Use:   "scenario",
	Short: "Run the anglova scenario",
	Long:  "Run the anglova scenario and publish Blue Force Tracks, Sensor Data, HQ Reports",
	Run: func(cmd *cobra.Command, args []string) {

		a := scenario.NewAnglova(cfg)
		log.Info("Running Anglova scenario...")
		a.Run()

	},
}
