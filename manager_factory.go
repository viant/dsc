package dsc

import (
	"fmt"
)

type managerFactoryProxy struct{}

// Create creates a new manager for the passed in config.
func (f managerFactoryProxy) Create(config *Config) (Manager, error) {
	if config.DriverName == "" && config.Driver == "" {
		return nil, fmt.Errorf("DriverName was empty %v", config)
	}
	factory, err := GetManagerFactory(config.DriverName)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup manager factory for `%v`, make sure you have imported required implmentation", config.DriverName)
	}
	config.Init()
	return factory.Create(config)
}

// CreateFromURL create a new manager from URL, url resource should be a JSON Config
func (f managerFactoryProxy) CreateFromURL(URL string) (Manager, error) {
	config, err := NewConfigFromURL(URL)
	if err != nil {
		return nil, err
	}
	factory, err := GetManagerFactory(config.DriverName)
	if err != nil {
		return nil, fmt.Errorf("failed to lookup manager factory for `%v`, make sure you have imported required implmentation", config.DriverName)
	}
	return factory.Create(config)
}

// NewManagerFactory create a new manager factory.
func NewManagerFactory() ManagerFactory {
	var manager ManagerFactory = &managerFactoryProxy{}
	return manager
}
