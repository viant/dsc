/*
 *
 *
 * Copyright 2012-2016 Viant.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 *  use m file except in compliance with the License. You may obtain a copy of
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
package dsc

import (
	"database/sql"
	"fmt"
	"bufio"
	"strings"
	"github.com/viant/toolbox"
	"path"
	"os"
	"sync"
	"bytes"
	"reflect"
)

var defaultPermission os.FileMode = 0644


//FileManager represents a line delimiter, JSON file manager.
// Current implementation is brute/force full file scan on each operation.
// Do not use it on production, it is not performance optimized.
// Idea behind is to be able to load data log and test it.
// You can easily add other managers providing your custom encoder and decoder factories i.e. protobuf, avro.
type FileManager struct {
	*AbstractManager
	encoderFactory toolbox.EncoderFactory
	decoderFactory toolbox.DecoderFactory
}

func (m *FileManager) convertIfNeeded(source interface{}) interface{} {
	sourceValue := reflect.ValueOf(source)
	if sourceValue.Kind() == reflect.Ptr {
		sourceValue = sourceValue.Elem()
	}

	if toolbox.IsTime(source) {
		dateLayout := m.config.GetDateLayout()
		return toolbox.AsTime(source, dateLayout).Format(dateLayout)
	}

	switch sourceValue.Kind() {


		case reflect.Slice:
			if sourceValue.Len() == 0 {
				return nil
			}

		case reflect.Map:
			if sourceValue.Len() == 0 {
				return nil
			}

		case reflect.Struct:
			var values = make(map[string]interface{})
			fieldsMappingByField := toolbox.NewFieldSettingByKey(source, "fieldName")

			toolbox.ProcessStruct(source,
				func(filed reflect.StructField, value interface{}) {
					mapping := fieldsMappingByField[filed.Name]
					column, found:=mapping["column"]
					if ! found {
						column = filed.Name
					}
					values[column] = m.convertIfNeeded(value)
				})
			return values
	}
	return source
}



func  getTableURL(manager Manager, table string) string {
	tableFile := table + "." + path.Join(manager.Config().Get("ext"))
	return path.Join(manager.Config().Get("url"), tableFile)
}

func (m *FileManager) encodeRecord(record map[string]interface{}) (string, error) {
	var buffer = new(bytes.Buffer)
	err := m.encoderFactory.Create(buffer).Encode(&record)
	if err != nil {
		return "", fmt.Errorf("Failed to encode record: %v due to ", err)
	}
	result := string(buffer.Bytes())
	result = strings.Replace(result, "\n", "", len(result)) + "\n"
	return result, nil
}


func (m *FileManager) getRecord(statement *DmlStatement, parameters toolbox.Iterator) (map[string]interface{}, error) {
	record, err := statement.ColumnValueMap(parameters)
	if err != nil {
		return nil, err
	}
	for key, value := range record {
		value = m.convertIfNeeded(value)
		if value == nil {
			delete(record, key)
			continue
		}
		record[key] = value
	}
	return record, nil

}

func (m *FileManager) insertRecord(meteFileTable *meteFileTable, tableURL string, statement *DmlStatement, parameters toolbox.Iterator) error {
	if ! meteFileTable.exists() {
		err := meteFileTable.create()
		if err != nil {
			return fmt.Errorf("Failed to open table %v due to %v", tableURL, err)
		}
	}
	file, err := toolbox.OpenURL(tableURL, os.O_APPEND|os.O_WRONLY, defaultPermission)
	if err != nil {
		return fmt.Errorf("Failed to open table %v due to %v", tableURL, err)
	}
	defer file.Close()
	record, err  := m.getRecord(statement, parameters)
	if err != nil {
		return err
	}
	encodedRecord, err := m.encodeRecord(record)
	if err != nil {
		return err
	}
	_, err = file.WriteString(encodedRecord)
	if err != nil {
		return fmt.Errorf("Failed to write to table %v due to %v", tableURL, err)
	}
	return nil
}



func (m *FileManager) modifyRecords(tableURL string, statement *DmlStatement, parameters toolbox.Iterator,  onMatchedHandler func(record map[string]interface{}) (bool, error)) (int, error) {
	var count = 0;
	tempFile, err := toolbox.OpenURL(tableURL + ".swp", os.O_CREATE|os.O_WRONLY, defaultPermission)
	if err != nil {
		return 0, fmt.Errorf("Failed to write to table %v due to %v", tempFile.Name(), err)
	}
	var predicate toolbox.Predicate
	if len(statement.Criteria) > 0 {
		predicate, err = NewSQLCriteriaPredicate(parameters, statement.Criteria...)
		if err != nil {
			return 0, fmt.Errorf("Failed to read data from %v due to %v",statement.SQL, err)
		}
	}
	err = m.fetchRecords(statement.Table, predicate, func(record map[string]interface{}, matched bool) (bool, error) {

		if matched {
			count++
			processRecord, err := onMatchedHandler(record);
			if err != nil {
				return false, err
			}
			if ! processRecord {
				return true, nil //continue process next rows
			}
		}
		encodedRecord, err := m.encodeRecord(record)
		if err != nil {
			return false, err
		}
		_, err = tempFile.WriteString(encodedRecord)
		if err != nil {
			return false, err
		}
		return true, nil
	})
	tempFile.Close()

	if err != nil {
		return 0, fmt.Errorf("Failed to modify %v due to %v", statement.Table, err)
	}
	if err == nil {
		file, err := toolbox.FileFromURL(tableURL)
		err = os.Rename(tempFile.Name(), file)
		if err != nil {
			return 0, fmt.Errorf("Unable write changes %v to %v due to %v", tempFile.Name(), file, err)
		}
	}
	if err != nil {
		return 0, fmt.Errorf("Failed to modify %v due to %v", statement.Table, err)
	}
	return count, nil

}



func (m *FileManager) updateRecords(tableURL string, statement *DmlStatement, parameters toolbox.Iterator) (int, error) {
	updatedRecord, err := m.getRecord(statement, parameters)
	if err != nil {
		return 0, fmt.Errorf("Failed to update table %v, due to %v", statement.Table, err)
	}
	return m.modifyRecords(tableURL, statement, parameters, func(record map[string]interface{}) (bool, error) {
		for k, v := range updatedRecord {
			record[k] = v
		}
		return true, nil
	})
}

func (m *FileManager) deleteRecords(tableURL string, statement *DmlStatement, parameters toolbox.Iterator) (int, error) {
	return m.modifyRecords(tableURL, statement, parameters, func(record map[string]interface{}) (bool, error) {
		return false, nil
	})
}

//ExecuteOnConnection executs passed in sql on connection. It takes connection, sql and sql parameters. It returns number of rows affected, or error.
//This method support basic insert, updated and delete operations.
func (m *FileManager) ExecuteOnConnection(connection Connection, sql string, sqlParameters []interface{}) (sql.Result, error) {
	parser := NewDmlParser()
	statement, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	tableURL := getTableURL(m, statement.Table)
	metaFileTable := staticMetaFileTableRegistry.get(tableURL, statement.Table)
	metaFileTable.Lock()
	defer metaFileTable.Unlock()
	var count = 0
	parameters := toolbox.NewSliceIterator(sqlParameters)
	switch (statement.Type) {
	case "INSERT":
		err = m.insertRecord(metaFileTable, tableURL, statement, parameters)
		if err == nil {
			count = 1
		}
	case "UPDATE":
		count, err = m.updateRecords(tableURL, statement, parameters)
	case "DELETE":
		count, err = m.deleteRecords(tableURL, statement, parameters)
	}
	if err != nil {
		return nil, err
	}
	return NewSQLResult(int64(count), 0), nil
}

func (m *FileManager) fetchRecords(table string, predicate toolbox.Predicate, recordHandler func(record map[string]interface{}, matched bool) (bool, error)) error {
	tableURL := getTableURL(m, table)
	metaFileTable := staticMetaFileTableRegistry.get(tableURL, table)
	if ! metaFileTable.exists() {
		return nil
	}
	reader, _, err := toolbox.OpenReaderFromURL(tableURL)
	if err != nil {
		return err
	}
	defer reader.Close()
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		line := scanner.Text()
		decoder := m.decoderFactory.Create(strings.NewReader(line))
		record := make(map[string]interface{})
		err := decoder.Decode(&record)
		if err != nil {
			return fmt.Errorf("Failed to decode record from %v due to %v \n%v\n", table, err, line)
		}
		matched := true
		if predicate != nil {
			matched = predicate.Apply(record)
		}
		toContinue, err := recordHandler(record, matched)
		if err != nil {
			return fmt.Errorf("Failed to fetch records due to %v", err)
		}
		if ! toContinue {
			return nil
		}
	}
	return nil
}

func (m *FileManager) readWithPredicate(connection Connection, statement *QueryStatement, sqlParameters []interface{}, readingHandler func(scanner Scanner) (toContinue bool, err error), predicate toolbox.Predicate) (error) {
	err := m.fetchRecords(statement.Table, predicate, func(record map[string]interface{}, matched bool) (bool, error) {
		if ! matched {
			return true, nil
		}
		var columns = make([]string, 0)
		if statement.Columns != nil && len(statement.Columns) > 0 {
			for _, column := range statement.Columns {
				columns = append(columns, column.Name)
			}
		} else {
			columns = toolbox.MapKeysToStringSlice(record)
		}
		fileScanner := NewFileScanner(m.config, columns)
		fileScanner.Values = record
		var scanner Scanner = fileScanner
		toContinue, err := readingHandler(scanner)
		if err != nil {
			return false, fmt.Errorf("Failed to read data on statement %v, due to\n\t%v", statement.SQL, err)
		}
		if ! toContinue {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	return nil
}

//ReadAllOnWithHandlerOnConnection reads all records on passed in connection.
func (m *FileManager) ReadAllOnWithHandlerOnConnection(connection Connection, query string, sqlParameters []interface{}, readingHandler func(scanner Scanner) (toContinue bool, err error)) error {
	parser := NewQueryParser()
	statement, err := parser.Parse(query)
	if err != nil {
		return fmt.Errorf("Failed to parse statement %v, %v", query, err)
	}
	var predicate toolbox.Predicate
	if len(statement.Criteria) > 0 {
		parameters := toolbox.NewSliceIterator(sqlParameters)
		predicate, err = NewSQLCriteriaPredicate(parameters, statement.Criteria...)
		if err != nil {
			return fmt.Errorf("Failed to read data from %v due to %v",query, err)
		}
	}
	return m.readWithPredicate(connection, statement, sqlParameters, readingHandler, predicate)
}


//NewFileManager creates a new file manager.
func NewFileManager(encoderFactory toolbox.EncoderFactory, decoderFactory toolbox.DecoderFactory) *FileManager {
	return &FileManager{
		encoderFactory :encoderFactory,
		decoderFactory :decoderFactory,
	}
}


type meteFileTable struct {
	sync.RWMutex
	table string
	url   string
}


func (m *meteFileTable) exists() bool {
	filePath, err := toolbox.FileFromURL(m.url)
	if err != nil {
		return false
	}
	_, err = os.Stat(filePath)
	if err != nil {
		return false
	}
	return true
}


func (m *meteFileTable) create() error{
	filePath, err := toolbox.OpenURL(m.url, os.O_CREATE, defaultPermission)
	if err != nil {
		return err
	}
	filePath.Close()
	return nil
}

type metaFileTableRegistry struct {
	sync.RWMutex
	registry map[string]*meteFileTable
}

func (r *metaFileTableRegistry) get(url string, table string) *meteFileTable {
	r.Lock()
	defer r.Unlock()
	result, found := r.registry[url];

	if found {
		return result
	}
	result = &meteFileTable{url:url, table:table}
	r.registry[url] = result
	return result
}

func newMetaFileTableRegistry() *metaFileTableRegistry {
	return &metaFileTableRegistry{registry:make(map[string]*meteFileTable)}
}

var staticMetaFileTableRegistry = newMetaFileTableRegistry()
