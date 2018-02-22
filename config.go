package dsc

import (
	"strings"
	"github.com/viant/toolbox"
	"time"
	"github.com/viant/toolbox/cred"
	"github.com/viant/toolbox/url"
)

//Config represent datastore config.
type Config struct {
	DriverName  string
	PoolSize    int
	MaxPoolSize int
	Descriptor  string
	Parameters  map[string]string
	Credential  string
}

//Get returns value for passed in parameter name or panic - please use Config.Has to check if value is present.
func (c *Config) Get(name string) string {
	if result, ok := c.Parameters[name]; ok {
		return result
	}
	return ""
}

//GetInt returns value for passed in parameter name or defaultValue
func (c *Config) GetInt(name string, defaultValue int) int {
	if result, ok := c.Parameters[name]; ok {
		return toolbox.AsInt(result)
	}
	return defaultValue
}

//GetFloat returns value for passed in parameter name or defaultValue
func (c *Config) GetFloat(name string, defaultValue float64) float64 {
	if result, ok := c.Parameters[name]; ok {
		return toolbox.AsFloat(result)
	}
	return defaultValue
}

//GetDuration returns value for passed in parameter name or defaultValue
func (c *Config) GetDuration(name string, multiplier time.Duration, defaultValue time.Duration) time.Duration {
	if result, ok := c.Parameters[name]; ok {
		return time.Duration(toolbox.AsInt(result)) * multiplier
	}
	return defaultValue
}

//GetString returns value for passed in parameter name or defaultValue
func (c *Config) GetString(name string, defaultValue string) string {
	if result, ok := c.Parameters[name]; ok {
		return result
	}
	return defaultValue
}

//GetBoolean returns value for passed in parameter name or defaultValue
func (c *Config) GetBoolean(name string, defaultValue bool) bool {
	if result, ok := c.Parameters[name]; ok {
		return toolbox.AsBoolean(result)
	}
	return defaultValue
}

//HasDateLayout returns true if config has date layout, it checks dateLayout or dateFormat parameter names.
func (c *Config) HasDateLayout() bool {
	return toolbox.HasTimeLayout(c.Parameters)
}

//GetDateLayout returns date layout
func (c *Config) GetDateLayout() string {
	return toolbox.GetTimeLayout(c.Parameters)
}

//Has returns true if parameter with passed in name is present, otherwise it returns false.
func (c *Config) Has(name string) bool {
	if _, ok := c.Parameters[name]; ok {
		return true
	}
	return false
}

//Init makes parameter map from encoded parameters if presents, expands descriptor with parameter value using [param_name] matching pattern.
func (c *Config) Init() error {
	if c.Credential != "" {
		config, err := cred.NewConfig(c.Credential)
		if err != nil {
			return err
		}
		c.Descriptor = strings.Replace(c.Descriptor, "[username]", config.Username, 1)
		c.Descriptor = strings.Replace(c.Descriptor, "[password]", config.Password, 1)
	}
	for key, value := range c.Parameters {
		macro := "[" + key + "]"
		c.Descriptor = strings.Replace(c.Descriptor, macro, value, 1)
	}
	return nil
}

//NewConfig creates new Config, it takes the following parameters
// descriptor - optional datastore connection string with macros that will be looked epxanded from for instance [user]:[password]@[url]
// encodedParameters should be in the following format:   <key1>:<value1>, ...,<keyN>:<valueN>
func NewConfig(driverName string, descriptor string, encodedParameters string) *Config {
	var parameters = toolbox.MakeStringMap(encodedParameters, ":", ",")
	result := &Config{DriverName: driverName, PoolSize: 1, MaxPoolSize: 2, Descriptor: descriptor, Parameters: parameters}
	result.Init()
	return result
}

//NewConfigWithParameters creates a new config with parameters
func NewConfigWithParameters(driverName string, descriptor string, parameters map[string]string) (*Config, error) {
	result := &Config{
		DriverName: driverName,
		Descriptor: descriptor,
		Parameters: parameters,
	}
	err := result.Init()
	return result, err
}

//NewConfigFromUrl returns new config from url
func NewConfigFromURL(URL string) (*Config, error) {
	result := &Config{}
	var resource = url.NewResource()
	err := resource.JSONDecode(result)
	if err == nil {
		err = result.Init()
	}
	return result, err
}
