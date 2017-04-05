package config

import "time"

// handles configuration for a specific protocol
type Manager struct {
	BrokerPort       string
	BrokerAddress    string
	StatsAddress     string
	StatsPort        string
	Protocol         string
	MessageSize      int
	PublishInterval  time.Duration
	MessageNumber    int
	Topic            string
	Scheme           string
	File             string //config file
	EnableBlueForce  bool
	EnableSensorData bool
	EnableHQReport   bool
}
