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
	URL                 string
	DriverName          string
	PoolSize            int
	MaxPoolSize         int
	Descriptor          string
	Parameters          map[string]interface{}
	Credentials         string
	MaxRequestPerSecond int
	cred                string
	username            string
	password            string
	dsnDescriptor       string
	lock                *sync.Mutex
	race                uint32
	initRun             bool
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

//DsnDescriptor return dsn expanded descriptor or error
func (c *Config) DsnDescriptor() (string, error) {
	if c.dsnDescriptor == "" {
		if err := c.Init(); err != nil {
			return "", err
		}
	}
	return c.dsnDescriptor, nil
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

func (c *Config) loadCredentials() error {
	if c.Credentials == "" {
		return nil
	}
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
	c.password = config.Password
	c.Parameters["username"] = c.username
	return nil
}

//Init makes parameter map from encoded parameters if presents, expands descriptor with parameter value using [param_name] matching pattern.
func (c *Config) Init() error {
	defer func() { c.initRun = true }()
	if c.cred == "" {
		c.cred = c.Credentials
	}
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
	if err := c.loadCredentials(); err != nil {
		return err
	}
	c.dsnDescriptor = c.Descriptor
	c.dsnDescriptor = strings.Replace(c.dsnDescriptor, "[username]", c.username, 1)
	c.dsnDescriptor = strings.Replace(c.dsnDescriptor, "[password]", c.password, 1)
	for key, value := range c.Parameters {
		textValue, ok := value.(string)
		if !ok {
			continue
		}
		macro := "[" + key + "]"
		c.dsnDescriptor = strings.Replace(c.dsnDescriptor, macro, textValue, 1)
	}
	return nil
}

//Clone clones config
func (c *Config) Clone() *Config {
	cred := c.cred
	if cred == "" {
		cred = c.Credentials
	}
	result := &Config{
		DriverName:          c.DriverName,
		URL:                 c.URL,
		Descriptor:          c.Descriptor,
		PoolSize:            c.PoolSize,
		MaxPoolSize:         c.MaxPoolSize,
		MaxRequestPerSecond: c.MaxRequestPerSecond,
		Parameters:          make(map[string]interface{}),
		username:            c.username,
		password:            c.password,
		dsnDescriptor:       c.dsnDescriptor,
		lock:                &sync.Mutex{},
		cred:                c.cred,
	}
	if len(c.Parameters) > 0 {
		for k, v := range c.Parameters {
			result.Parameters[k] = v
		}
	}
	return result
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
