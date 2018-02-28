package dsc

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"database/sql"
	"fmt"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/storage"
	"github.com/viant/toolbox/url"
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strings"
)

var defaultPermission os.FileMode = 0644

//FileManager represents a line delimiter, JSON file manager.
// Current implementation is brute/force full file scan on each operation.
// Do not use it on production, it is not performance optimized.
// Idea behind is to be able to load data log and test it.
// You can easily add other managers providing your custom encoder and decoder factories i.e. protobuf, avro.
type FileManager struct {
	baseURL *url.Resource
	*AbstractManager
	service             storage.Service
	useGzipCompressions bool
	hasHeaderLine       bool
	delimiter           string
	encoderFactory      toolbox.EncoderFactory
	decoderFactory      toolbox.DecoderFactory
}

func (m *FileManager) Init() error {
	m.baseURL = url.NewResource(m.Config().Get("url"))
	var err error
	m.service, err = storage.NewServiceForURL(m.baseURL.URL, m.config.Credential)
	extension := m.Config().Get("ext")
	m.useGzipCompressions = extension == "gzip"
	return err

}

func (m *FileManager) convertIfNeeded(source interface{}) interface{} {
	if source == nil {
		return nil
	}
	if toolbox.IsTime(source) {
		dateLayout := m.config.GetDateLayout()
		if dateLayout == "" {
			dateLayout = toolbox.DefaultDateLayout
		}
		var converted = toolbox.AsTime(source, dateLayout)
		if converted == nil {
			return nil
		}
		return converted.Format(dateLayout)
	}
	sourceValue := reflect.ValueOf(toolbox.DereferenceValue(source))
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
			func(filed reflect.StructField, value reflect.Value) error {
				mapping := fieldsMappingByField[filed.Name]
				column, found := mapping["column"]
				if !found {
					column = filed.Name
				}
				values[column] = m.convertIfNeeded(value.Interface())
				return nil
			})
		if len(values) == 0 {
			return toolbox.AsString(source)
		}
		return values
	}
	return source
}

func (m *FileManager) getTableURL(manager Manager, table string) string {
	tableFile := table + "." + path.Join(manager.Config().Get("ext"))
	if m.baseURL == nil {
		m.baseURL = url.NewResource(m.Config().Get("url"))
	}
	return toolbox.URLPathJoin(m.baseURL.URL, tableFile)
}

func (m *FileManager) encodeRecord(record map[string]interface{}, table string) (string, error) {
	var buffer = new(bytes.Buffer)
	var encoder = m.encoderFactory.Create(buffer)

	if m.delimiter != "" {
		descriptor := m.TableDescriptorRegistry().Get(table)
		encoder.Encode(descriptor.Columns)
	}

	err := encoder.Encode(&record)
	if err != nil {
		return "", fmt.Errorf("failed to encode record: %v due to ", err)
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

func (m *FileManager) insertRecord(connection Connection, tableURL string, statement *DmlStatement, parameters toolbox.Iterator) error {

	recordBuffer := new(bytes.Buffer)
	record, err := m.getRecord(statement, parameters)
	if err != nil {
		return err
	}
	encodedRecord, err := m.encodeRecord(record, statement.Table)
	if err != nil {
		return err
	}
	_, err = recordBuffer.Write(([]byte)(encodedRecord))
	if err != nil {
		return err
	}

	buf := new(bytes.Buffer)
	reader, err := m.getReaderForURL(tableURL)
	if reader != nil {
		defer reader.Close()
		_, err := io.Copy(buf, reader)
		if err != nil {
			return err
		}
	}
	buf.Write(recordBuffer.Bytes())
	return m.PersistTableData(tableURL, buf.Bytes())
}

func (m *FileManager) PersistTableData(tableURL string, data []byte) error {
	if m.useGzipCompressions {
		buffer := new(bytes.Buffer)
		writer := gzip.NewWriter(buffer)
		_, err := writer.Write(data)
		if err != nil {
			return err
		}
		err = writer.Flush()
		if err != nil {
			return fmt.Errorf("failed to compress data (flush) %v", err)
		}
		err = writer.Close()
		if err != nil {
			return fmt.Errorf("failed to compress data (close) %v", err)
		}
		data = buffer.Bytes()
	}
	return m.service.Upload(tableURL, bytes.NewReader(data))
}

func (m *FileManager) modifyRecords(tableURL string, statement *DmlStatement, parameters toolbox.Iterator, onMatchedHandler func(record map[string]interface{}) (bool, error)) (int, error) {
	var count = 0
	buf := new(bytes.Buffer)
	var err error
	var predicate toolbox.Predicate
	if len(statement.Criteria) > 0 {
		predicate, err = NewSQLCriteriaPredicate(parameters, statement.SQLCriteria)
		if err != nil {
			return 0, fmt.Errorf("failed to read data from %v due to %v", statement.SQL, err)
		}
	}
	err = m.fetchRecords(statement.Table, predicate, func(record map[string]interface{}, matched bool) (bool, error) {

		if matched {
			count++
			processRecord, err := onMatchedHandler(record)
			if err != nil {
				return false, err
			}
			if !processRecord {
				return true, nil //continue process next rows
			}
		}
		encodedRecord, err := m.encodeRecord(record, statement.Table)
		if err != nil {
			return false, err
		}

		buf.Write(([]byte)(encodedRecord))
		if err != nil {
			return false, err
		}
		return true, nil
	})
	err = m.PersistTableData(tableURL, buf.Bytes())
	return count, err
}

func (m *FileManager) updateRecords(tableURL string, statement *DmlStatement, parameters toolbox.Iterator) (int, error) {
	updatedRecord, err := m.getRecord(statement, parameters)
	if err != nil {
		return 0, fmt.Errorf("failed to update table %v, due to %v", statement.Table, err)
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
		return nil, fmt.Errorf("failed to parse sql: %v, %v", sql, err)

	}
	tableURL := m.getTableURL(m, statement.Table)

	var count = 0
	parameters := toolbox.NewSliceIterator(sqlParameters)
	switch statement.Type {
	case "INSERT":
		err = m.insertRecord(connection, tableURL, statement, parameters)
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

func (m *FileManager) getStorageObject(tableURL string) (storage.Object, error) {
	exists, err := m.service.Exists(tableURL)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	return m.service.StorageObject(tableURL)
}

func (m *FileManager) getReaderForURL(tableURL string) (io.ReadCloser, error) {
	object, err := m.getStorageObject(tableURL)
	if err != nil {
		return nil, err
	}
	if object == nil {
		return nil, nil
	}
	reader, err := m.service.Download(object)
	if err != nil {
		return nil, err
	}
	if reader != nil && m.useGzipCompressions {
		defer reader.Close()
		return gzip.NewReader(reader)
	}
	return reader, nil
}

func (m *FileManager) getRecordProvider(columns ...string) func() interface{} {
	if m.hasHeaderLine {
		return func() interface{} {
			record := make(map[string]interface{})
			return &DelimiteredRecord{
				Delimiter: m.delimiter,
				Record:    record,
				Columns:   columns,
			}
		}

	}
	return func() interface{} {
		var result = make(map[string]interface{})
		return &result
	}
}

func (m *FileManager) asFileRecordMap(source interface{}) map[string]interface{} {
	if m.hasHeaderLine {
		result, _ := source.(*DelimiteredRecord)
		return result.Record
	}
	result, _ := source.(*map[string]interface{})
	return *result
}

func (m *FileManager) readHeaderIfNeeded(scanner *bufio.Scanner) []string {
	if !m.hasHeaderLine {
		return []string{}
	}
	var headers []string
	if m.hasHeaderLine && scanner.Scan() {
		headerLine := scanner.Text()
		for _, column := range strings.Split(headerLine, m.delimiter) {
			headers = append(headers, strings.Trim(column, "\" "))
		}
	}
	return headers
}

func (m *FileManager) fetchRecords(table string, predicate toolbox.Predicate, recordHandler func(record map[string]interface{}, matched bool) (bool, error)) error {
	tableURL := m.getTableURL(m, table)
	reader, err := m.getReaderForURL(tableURL)
	if reader == nil {
		return err
	}
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	headers := m.readHeaderIfNeeded(scanner)
	var recordProvider = m.getRecordProvider(headers...)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		decoder := m.decoderFactory.Create(strings.NewReader(line))
		var err error
		record := recordProvider()
		err = decoder.Decode(record)
		if err != nil {
			return fmt.Errorf("failed to decode record from %v due to %v, line: %v", table, err, line)
		}
		recordMap := m.asFileRecordMap(record)
		matched := true
		if predicate != nil {
			matched = predicate.Apply(recordMap)
		}
		toContinue, err := recordHandler(recordMap, matched)
		if err != nil {
			return fmt.Errorf("failed to fetch records due to %v", err)
		}
		if !toContinue {
			return nil
		}
	}
	return nil
}

func (m *FileManager) readWithPredicate(connection Connection, statement *QueryStatement, sqlParameters []interface{}, readingHandler func(scanner Scanner) (toContinue bool, err error), predicate toolbox.Predicate) error {
	var columns = make([]string, 0)
	if statement.Columns != nil && len(statement.Columns) > 0 {
		for _, column := range statement.Columns {
			columns = append(columns, column.Name)
		}
	}
	fileScanner := NewFileScanner(m.config, columns)
	err := m.fetchRecords(statement.Table, predicate, func(record map[string]interface{}, matched bool) (bool, error) {
		if !matched {
			return true, nil
		}
		if len(columns) == 0 {
			columns = toolbox.MapKeysToStringSlice(record)
			fileScanner.columns = columns
		}
		fileScanner.Values = record
		toContinue, err := readingHandler(fileScanner)
		if err != nil {
			return false, fmt.Errorf("failed to read data on statement %v, due to\n\t%v", statement.SQL, err)
		}
		if !toContinue {
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
		return fmt.Errorf("failed to parse statement %v, %v", query, err)
	}
	var predicate toolbox.Predicate
	if len(statement.Criteria) > 0 {
		parameters := toolbox.NewSliceIterator(sqlParameters)
		predicate, err = NewSQLCriteriaPredicate(parameters, statement.SQLCriteria)
		if err != nil {
			return fmt.Errorf("failed to read data from %v due to %v", query, err)
		}
	}
	return m.readWithPredicate(connection, statement, sqlParameters, readingHandler, predicate)
}

//NewFileManager creates a new file manager.
func NewFileManager(encoderFactory toolbox.EncoderFactory, decoderFactory toolbox.DecoderFactory, valuesDelimiter string, config *Config) *FileManager {
	result := &FileManager{
		service:        storage.NewService(),
		delimiter:      valuesDelimiter,
		hasHeaderLine:  len(valuesDelimiter) > 0,
		encoderFactory: encoderFactory,
		decoderFactory: decoderFactory,
	}
	return result
}

type DelimiteredRecord struct {
	Columns   []string
	Delimiter string
	Record    map[string]interface{}
}

type delimiterDecoder struct {
	reader io.Reader
}

func (d *delimiterDecoder) Decode(target interface{}) error {
	delimiteredRecord, ok := target.(*DelimiteredRecord)
	if !ok {
		return fmt.Errorf("Invalid target type, expected %T but had %T", &DelimiteredRecord{}, target)
	}
	var isInDoubleQuote = false
	var index = 0
	var value = ""
	var delimiter = delimiteredRecord.Delimiter
	payload, err := ioutil.ReadAll(d.reader)
	if err != nil {
		return err
	}
	encoded := string(payload)
	for i := 0; i < len(encoded); i++ {
		if index >= len(delimiteredRecord.Columns) {
			break
		}

		aChar := string(encoded[i : i+1])
		//escape " only if value is already inside "s
		if isInDoubleQuote && aChar == "\\" {
			nextChar := encoded[i+1 : i+2]
			if nextChar == "\"" {
				i++
				value = value + nextChar
				continue
			}
		}
		//allow unescaped " be inside text if the whole text is not enclosed in "s
		if aChar == "\"" && (len(value) == 0 || isInDoubleQuote) {
			isInDoubleQuote = !isInDoubleQuote
			continue
		}
		if encoded[i:i+1] == delimiter && !isInDoubleQuote {
			var columnName = delimiteredRecord.Columns[index]
			delimiteredRecord.Record[columnName] = value
			value = ""
			index++
			continue
		}
		value = value + aChar
	}
	if len(value) > 0 {
		var columnName = delimiteredRecord.Columns[index]
		delimiteredRecord.Record[columnName] = value
	}
	return nil
}

type delimiterDecoderFactory struct{}

func (f *delimiterDecoderFactory) Create(reader io.Reader) toolbox.Decoder {
	return &delimiterDecoder{reader: reader}
}

type delimiterEncoderFactory struct {
	delimiter string
}

func (f *delimiterEncoderFactory) Create(writer io.Writer) toolbox.Encoder {
	return &delimiterEncoder{writer: writer}
}

type delimiterEncoder struct {
	writer    io.Writer
	delimiter string
	columns   []string
}

func (e *delimiterEncoder) Encode(object interface{}) error {
	if len(e.columns) == 0 {
		var ok bool
		e.columns, ok = object.([]string)
		if !ok {
			return fmt.Errorf("expected columns")
		}
		return nil
	}

	var objectMap = toolbox.AsMap(object)
	var values = make([]string, 0)
	for _, column := range e.columns {
		candidate := objectMap[column]
		var value string
		escapeValue := true
		if toolbox.IsMap(candidate) || toolbox.IsSlice(candidate) {
			var valueBuffer = new(bytes.Buffer)
			err := toolbox.NewJSONEncoderFactory().Create(valueBuffer).Encode(candidate)
			if err != nil {
				return err
			}
			value = valueBuffer.String()
			value = strings.Replace(value, "\n", "", len(value))
		} else if toolbox.IsInt(candidate) || toolbox.IsFloat(candidate) {
			escapeValue = false
			value = toolbox.AsString(candidate)
		} else {
			value = toolbox.AsString(candidate)
		}
		if escapeValue {
			if strings.Contains(value, ",") || strings.Contains(value, "\"") {
				value = "\"" + strings.Replace(value, "\"", "\\\"", len(value)) + "\""
			}
		}
		values = append(values, value)
	}
	e.writer.Write([]byte(strings.Join(values, ",")))
	return nil
}
