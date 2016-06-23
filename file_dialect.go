/*
 *
 *
 * Copyright 2012-2016 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use this file except in compliance with the License. You may obtain a copy of
 *  the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 *  License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

// Package dsc - File dialect
package dsc

import (
	"io/ioutil"
	"os"
	"path"

	"github.com/viant/toolbox"
)

type fileDialect struct {
	defaultDialect
}

//DropTable drops a table in datastore managed by passed in manager.
func (d fileDialect) DropTable(manager Manager, datastore string, table string) error {
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
	for _, file := range files {
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
