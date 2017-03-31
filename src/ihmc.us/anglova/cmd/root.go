// Copyright © 2017 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"os"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"ihmc.us/anglova/config"
)

//version
var (
	Version   string
	BuildTime string
	GitHash   string
)

const (
	Authors = "Enrico Casini <ecasini@ihmc.us>, " +
		"Filippo Poltronieri <fpoltronier@ihmc.us>, " +
		"Emanuele Tagliaferro <etagliaferro@ihmc.us>"
	Name       = "anglova"
	DescrShort = "A benchmark platform for dissemination protocols"
	DescrLong  = "Anglova is a benchmark platform that enables the evaluation " +
		"of a variety of messaging protocols and P2P file systems: " +
		"NATS, RabbitMQ, Apache Kafka, MQTT, IPFS etc."
	DescrSignature         = Name + " - " + DescrShort
	DefaultBrokerAddress   = "127.0.0.1"
	DefaultProtocol        = "nats"
	DefaultMessageSize     = 1024 //in bytes
	DefaultPublishInterval = 1000 //in msec
	DefaultMessageNumber   = 1024 //number of messages to be sent in a session
	DefaultTopic           = "test"
	DefaultPort            = "4222"
	Scheme                 = "://"
)

// RootCmd represents the base command when called without any sub commands
var RootCmd = &cobra.Command{
	Use:   Name,
	Short: DescrShort,
	Long:  DescrLong,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	//	Run: func(cmd *cobra.Command, args []string) { },
}

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

//config manager
var cfg config.Manager

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports Persistent Flags, which, if defined here,
	// will be global for your application.

	cfg = config.Manager{}

	RootCmd.PersistentFlags().StringVar(&cfg.File, "config", "",
		"config file (default is $HOME/."+Name+".yaml)")
	RootCmd.PersistentFlags().StringVar(&cfg.BrokerAddress, "broker-address", DefaultBrokerAddress, "broker address")
	RootCmd.PersistentFlags().StringVar(&cfg.StatsAddress, "stats-address", DefaultBrokerAddress, "statistics server address")
	RootCmd.PersistentFlags().StringVar(&cfg.Protocol, "protocol", DefaultProtocol, "protocol type")
	RootCmd.PersistentFlags().IntVar(&cfg.MessageSize, "message-size", DefaultMessageSize, "size of the message")
	RootCmd.PersistentFlags().DurationVar(&cfg.PublishInterval, "publish-interval", DefaultPublishInterval,
		"interval between two messages published")
	RootCmd.PersistentFlags().IntVar(&cfg.MessageNumber, "message-number", DefaultMessageNumber, "number of messages")
	RootCmd.PersistentFlags().StringVar(&cfg.Port, "port", DefaultPort, "the broker's port")
	RootCmd.PersistentFlags().StringVar(&cfg.Topic, "topic", "test", "topic name")
	RootCmd.PersistentFlags().Bool("viper", true, "Use Viper for configuration")
	viper.BindPFlag("author", RootCmd.PersistentFlags().Lookup("author"))
	viper.BindPFlag("projectbase", RootCmd.PersistentFlags().Lookup("projectbase"))
	viper.BindPFlag("useViper", RootCmd.PersistentFlags().Lookup("viper"))
	viper.SetDefault("author", Authors)
	viper.SetDefault("license", "apache")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	RootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfg.File != "" { // enable ability to specify config file via flag
		viper.SetConfigFile(cfg.File)
	}

	viper.SetConfigName("." + Name) // name of config file (without extension)
	viper.AddConfigPath("$HOME")    // adding home directory as first search path
	viper.AutomaticEnv()            // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file:", viper.ConfigFileUsed())
	}
}
