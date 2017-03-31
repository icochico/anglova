package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
)

func init() {
	RootCmd.AddCommand(versionCmd)
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version number of anglova",
	Long:  `All software has versions. This is anglova's`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("Version: " + Version)
		fmt.Println("Build Time: " + BuildTime)
		fmt.Println("Git Hash: " + GitHash)
	},
}
