package config

import "time"

// handles configuration for a specific protocol
type Manager struct {
	Host            string
	Port            string
	BrokerAddress   string
	StatsAddress    string
	Protocol        string
	MessageSize     int
	PublishInterval time.Duration
	MessageNumber   int
	Topic           string
	Scheme          string
	File            string //config file
}
