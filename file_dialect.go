package dsc

import (
	"fmt"
	"net/url"
	"path"
	"strings"
)

type fileDialect struct {
	DefaultDialect
}

//DropTable drops a table in datastore managed by passed in manager.
func (d fileDialect) DropTable(manager Manager, datastore string, table string) error {
	fileManager, ok := manager.(*FileManager)
	if !ok {
		return fmt.Errorf("invalid store manager: %T, expected %T", &FileManager{}, manager)
	}
	tableURL := getTableURL(manager, table)
	exists, err := fileManager.service.Exists(tableURL)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	object, err := fileManager.service.StorageObject(tableURL)
	if err != nil {
		return err
	}
	if object == nil {
		return nil
	}
	return fileManager.service.Delete(object)
}

//GetTables return tables names for passed in datastore managed by passed in manager.
func (d fileDialect) GetTables(manager Manager, datastore string) ([]string, error) {
	fileManager, ok := manager.(*FileManager)
	if !ok {
		return nil, fmt.Errorf("Invalid store manager: %T, expected %T", &FileManager{}, manager)
	}
	baseURL := manager.Config().Get("url")

	exists, err := fileManager.service.Exists(baseURL)
	if err != nil {
		return nil, err
	}
	if !exists {
		return []string{}, nil
	}

	objects, err := fileManager.service.List(baseURL)
	ext := "." + manager.Config().Get("ext")
	var result = make([]string, 0)
	for _, object := range objects {
		if object.IsFolder() {
			continue
		}
		parsedURL, err := url.Parse(object.URL())
		if err != nil {
			return nil, err
		}
		_, name := path.Split(parsedURL.Path)
		if strings.HasSuffix(name, ext) {
			result = append(result, name)
		}
	}
	return result, nil
}

//GetCurrentDatastore returns  url, base path
func (d fileDialect) GetCurrentDatastore(manager Manager) (string, error) {
	return manager.Config().Get("url"), nil
}
