package dsc

import (
	"encoding/json"
	"fmt"

	"github.com/viant/toolbox"
)

type managerFactoryProxy struct{}

//Create creates a new manager for the passed in config.
func (f managerFactoryProxy) Create(config *Config) (Manager, error) {
	if config.DriverName == "" {
		return nil, fmt.Errorf("DriverName was empty %v", config)
	}
	factory, err := GetManagerFactory(config.DriverName)
	if err != nil {
		return nil, fmt.Errorf("Failed to lookup manager factory for `%v`, make sure you have imported required implmentation", config.DriverName)
	}
	return factory.Create(config)
}

//CreateFromURL create a new manager from URL, url resource should be a JSON Config
func (f managerFactoryProxy) CreateFromURL(url string) (Manager, error) {
	reader, _, err := toolbox.OpenReaderFromURL(url)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	config := &Config{}
	err = json.NewDecoder(reader).Decode(config)
	if err != nil {
		return nil, err
	}
	config.Init()
	return f.Create(config)
}

//NewManagerFactory create a new manager factory.
func NewManagerFactory() ManagerFactory {
	var manager ManagerFactory = &managerFactoryProxy{}
	return manager
}
