package dsc


type sqlManagerFactory struct{}

func (mf *sqlManagerFactory) Create(config *Config) (Manager, error) {
	var connectionProvider = newSQLConnectionProvider(config)
	sqlManager := &sqlManager{}
	var self Manager = sqlManager
	super := NewAbstractManager(config, connectionProvider, self)
	sqlManager.AbstractManager = super
	return self, nil
}

func (mf sqlManagerFactory) CreateFromURL(URL string) (Manager, error) {
	config, err := NewConfigFromURL(URL)
	if err != nil {
		return nil, err
	}
	return mf.Create(config)
}
