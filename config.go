package dsc

import (
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/secret"
	"github.com/viant/toolbox/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

//BatchSizeKey represents a config batch size parameter
const BatchSizeKey = "batchSize"

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
	lock             *sync.Mutex
	race             uint32
	initRun          bool
}

//Get returns value for passed in parameter name or panic - please use Config.Has to check if value is present.
func (c *Config) Get(name string) string {
	c.lock.Lock()
	defer c.lock.Unlock()
	if result, ok := c.Parameters[name]; ok {
		return toolbox.AsString(result)
	}
	return ""
}

//Get returns value for passed in parameter name or panic - please use Config.Has to check if value is present.
func (c *Config) GetMap(name string) map[string]interface{} {
	c.lock.Lock()
	defer c.lock.Unlock()
	if result, ok := c.Parameters[name]; ok {
		if toolbox.IsMap(result) {
			return toolbox.AsMap(result)
		}
	}
	return nil
}

//GetInt returns value for passed in parameter name or defaultValue
func (c *Config) GetInt(name string, defaultValue int) int {
	c.lock.Lock()
	defer c.lock.Unlock()
	if result, ok := c.Parameters[name]; ok {
		return toolbox.AsInt(result)
	}
	return defaultValue
}

//GetFloat returns value for passed in parameter name or defaultValue
func (c *Config) GetFloat(name string, defaultValue float64) float64 {
	c.lock.Lock()
	defer c.lock.Unlock()
	if result, ok := c.Parameters[name]; ok {
		return toolbox.AsFloat(result)
	}
	return defaultValue
}

//GetDuration returns value for passed in parameter name or defaultValue
func (c *Config) GetDuration(name string, multiplier time.Duration, defaultValue time.Duration) time.Duration {
	c.lock.Lock()
	defer c.lock.Unlock()
	if result, ok := c.Parameters[name]; ok {
		return time.Duration(toolbox.AsInt(result)) * multiplier
	}
	return defaultValue
}

//GetString returns value for passed in parameter name or defaultValue
func (c *Config) GetString(name string, defaultValue string) string {
	c.lock.Lock()
	defer c.lock.Unlock()
	if result, ok := c.Parameters[name]; ok {
		return toolbox.AsString(result)
	}
	return defaultValue
}

//GetBoolean returns value for passed in parameter name or defaultValue
func (c *Config) GetBoolean(name string, defaultValue bool) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if result, ok := c.Parameters[name]; ok {
		return toolbox.AsBoolean(result)
	}
	return defaultValue
}

//HasDateLayout returns true if config has date layout, it checks dateLayout or dateFormat parameter names.
func (c *Config) HasDateLayout() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return toolbox.HasTimeLayout(c.Parameters)
}

//GetDateLayout returns date layout
func (c *Config) GetDateLayout() string {
	c.lock.Lock()
	defer c.lock.Unlock()
	return toolbox.GetTimeLayout(c.Parameters)
}

//Has returns true if parameter with passed in name is present, otherwise it returns false.
func (c *Config) Has(name string) bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	if _, ok := c.Parameters[name]; ok {
		return true
	}
	return false
}

func (c *Config) initMutextIfNeeed() {
	if c.lock == nil {
		if atomic.CompareAndSwapUint32(&c.race, 0, 1) {
			c.lock = &sync.Mutex{}
		} else {
			c.initMutextIfNeeed()
		}
	}
}

//Init makes parameter map from encoded parameters if presents, expands descriptor with parameter value using [param_name] matching pattern.
func (c *Config) Init() error {
	defer func() { c.initRun = true }()
	c.initMutextIfNeeed()
	if c.URL != "" && c.DriverName == "" {
		resource := url.NewResource(c.URL)
		if err := resource.Decode(c); err != nil {
			return err
		}
	}

	var lock = c.lock
	lock.Lock()
	defer lock.Unlock()

	if c.Credentials != "" {
		secrets := secret.New("", false)
		config, err := secrets.GetCredentials(c.Credentials)
		if err != nil {
			return err
		}
		if len(c.Parameters) == 0 {
			c.Parameters = make(map[string]interface{})
		}
		if location, err := secrets.CredentialsLocation(c.Credentials); err == nil {
			c.Credentials = location
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
	result := &Config{DriverName: driverName, PoolSize: 1, MaxPoolSize: 2, Descriptor: descriptor, Parameters: parameters,
		lock: &sync.Mutex{}}
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
		lock:        &sync.Mutex{},
	}
	err := result.Init()
	return result, err
}

//NewConfigFromUrl returns new config from url
func NewConfigFromURL(URL string) (*Config, error) {
	result := &Config{}
	var resource = url.NewResource(URL)
	err := resource.Decode(result)
	result.lock = &sync.Mutex{}
	if err == nil {
		err = result.Init()
	}

	return result, err
}
