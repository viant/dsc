package dsc

import (
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/secret"
	"github.com/viant/toolbox/url"
	"strings"
	"time"
)

//Config represent datastore config.
type Config struct {
	URL              string
	DriverName       string
	PoolSize         int
	MaxPoolSize      int
	Descriptor       string
	SecureDescriptor string
	Parameters       map[string]interface{}
	Credentials      string
	username         string
}

//Get returns value for passed in parameter name or panic - please use Config.Has to check if value is present.
func (c *Config) Get(name string) string {
	if result, ok := c.Parameters[name]; ok {
		return toolbox.AsString(result)
	}
	return ""
}

//Get returns value for passed in parameter name or panic - please use Config.Has to check if value is present.
func (c *Config) GetMap(name string) map[string]interface{} {
	if result, ok := c.Parameters[name]; ok {
		if toolbox.IsMap(result) {
			return toolbox.AsMap(result)
		}
	}
	return nil
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
		return toolbox.AsString(result)
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
	if c.URL != "" && c.DriverName == "" {
		resource := url.NewResource(c.URL)
		if err := resource.Decode(c); err != nil {
			return err
		}
	}
	if c.Credentials != "" {
		secrets := secret.New("", false)
		config, err := secrets.GetCredentials(c.Credentials)
		if err != nil {
			return err
		}
		if len(c.Parameters) == 0 {
			c.Parameters = make(map[string]interface{})
		}
		c.username = config.Username
		c.SecureDescriptor = c.Descriptor
		c.Parameters["username"] = config.Username
		c.Parameters["password"] = config.Password
		c.SecureDescriptor = strings.Replace(c.SecureDescriptor, "[password]", "***", 1)
	}

	for key, value := range c.Parameters {
		textValue, ok := value.(string)
		if !ok {
			continue
		}
		macro := "[" + key + "]"
		c.Descriptor = strings.Replace(c.Descriptor, macro, textValue, 1)
		c.SecureDescriptor = strings.Replace(c.SecureDescriptor, macro, textValue, 1)
	}
	return nil
}

//NewConfig creates new Config, it takes the following parameters
// descriptor - optional datastore connection string with macros that will be looked epxanded from for instance [user]:[password]@[url]
// encodedParameters should be in the following format:   <key1>:<value1>, ...,<keyN>:<valueN>
func NewConfig(driverName string, descriptor string, encodedParameters string) *Config {
	var parameters = toolbox.MakeMap(encodedParameters, ":", ",")
	result := &Config{DriverName: driverName, PoolSize: 1, MaxPoolSize: 2, Descriptor: descriptor, Parameters: parameters}
	result.Init()
	return result
}

//NewConfigWithParameters creates a new config with parameters
func NewConfigWithParameters(driverName string, descriptor string, credential string, parameters map[string]interface{}) (*Config, error) {
	result := &Config{
		DriverName:  driverName,
		Descriptor:  descriptor,
		Credentials: credential,
		Parameters:  parameters,
	}
	err := result.Init()
	return result, err
}

//NewConfigFromUrl returns new config from url
func NewConfigFromURL(URL string) (*Config, error) {
	result := &Config{}
	var resource = url.NewResource(URL)
	err := resource.Decode(result)
	if err == nil {
		err = result.Init()
	}
	return result, err
}
