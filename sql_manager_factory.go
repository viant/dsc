package dsc

import "github.com/viant/toolbox"

type sqlManagerFactory struct{}

func (mf *sqlManagerFactory) Create(config *Config) (Manager, error) {
	var connectionProvider = newSQLConnectionProvider(config)
	sqlManager := &sqlManager{}
	var self Manager = sqlManager
	super := NewAbstractManager(config, connectionProvider, self)
	sqlManager.AbstractManager = super
	return self, nil
}

func (mf sqlManagerFactory) CreateFromURL(url string) (Manager, error) {
	reader, _, err := toolbox.OpenReaderFromURL(url)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	config := &Config{}
	return mf.Create(config)
}
