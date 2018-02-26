package dsc

import (
	"github.com/viant/toolbox"
)

type jsonFileManagerFactory struct{}

func (f *jsonFileManagerFactory) Create(config *Config) (Manager, error) {
	var connectionProvider = newFileConnectionProvider(config)
	fileManager := NewFileManager(toolbox.NewJSONEncoderFactory(), toolbox.NewJSONDecoderFactory(), "", config)
	super := NewAbstractManager(config, connectionProvider, fileManager)
	fileManager.AbstractManager = super
	err := fileManager.Init()
	if err != nil {
		return nil, err
	}
	return fileManager, nil
}

func (f jsonFileManagerFactory) CreateFromURL(URL string) (Manager, error) {
	config, err := NewConfigFromURL(URL)
	if err != nil {
		return nil, err
	}
	return f.Create(config)
}

type delimiteredFileManagerFactory struct {
	delimiter string
}

func (f *delimiteredFileManagerFactory) Create(config *Config) (Manager, error) {
	var connectionProvider = newFileConnectionProvider(config)
	fileManager := NewFileManager(&delimiterEncoderFactory{delimiter: f.delimiter}, &delimiterDecoderFactory{}, f.delimiter, config)
	super := NewAbstractManager(config, connectionProvider, fileManager)
	fileManager.AbstractManager = super
	return fileManager, nil
}

func (f delimiteredFileManagerFactory) CreateFromURL(URL string) (Manager, error) {
	config, err := NewConfigFromURL(URL)
	if err != nil {
		return nil, err
	}
	return f.Create(config)
}
