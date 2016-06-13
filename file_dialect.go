package dsc

import (
	"github.com/viant/toolbox"
	"os"
	"io/ioutil"
	"path"
)

type fileDialect struct {
	defaultDialect
}


//DropTable drops a table in datastore managed by passed in manager.
func (d fileDialect) DropTable(manager Manager, datastore string, table string) (error) {
	file, err := toolbox.FileFromURL(getTableURL(manager, table))
	if err != nil {
		return err
	}
	return os.Remove(file)
}


//GetTables return tables names for passed in datastore managed by passed in manager.
func (d fileDialect) GetTables(manager Manager, datastore string) ([]string, error) {
	basePath, err := toolbox.FileFromURL(manager.Config().Get("url"))
	ext := "." + manager.Config().Get("ext")
	if err != nil {
		return nil, err
	}
	files, err := ioutil.ReadDir(basePath)
	if err != nil {
		return nil, err
	}
	var result = make([]string, 0)
	for _, file  :=range files {
		if path.Ext(file.Name()) == ext {
			result = append(result, file.Name())
		}
	}
	return result, nil
}

//GetCurrentDatastore returns  url, base path
func (d fileDialect) GetCurrentDatastore(manager Manager) (string, error) {
	return manager.Config().Get("url"), nil
}
