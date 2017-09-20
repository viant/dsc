package dsc

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"database/sql"
	"fmt"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/storage"
	"github.com/viant/toolbox/storage/aws"
	"github.com/viant/toolbox/storage/gs"
	"google.golang.org/api/option"
	"io"
	"io/ioutil"
	"net/url"
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
	*AbstractManager
	service             storage.Service
	useGzipCompressions bool
	hasHeaderLine       bool
	delimiter           string
	encoderFactory      toolbox.EncoderFactory
	decoderFactory      toolbox.DecoderFactory
}

func (m *FileManager) Init() error {
	var baseUrl = m.Config().Get("url")
	parsedUrl, err := url.Parse(baseUrl)
	if err != nil {
		return err
	}
	extension := m.Config().Get("ext")
	m.useGzipCompressions = extension == "gzip"
	switch parsedUrl.Scheme {
	case "file":
	case "gs":
		credential := option.WithServiceAccountFile(m.Config().Get("credential"))
		service := gs.NewService(credential)
		m.service.Register(parsedUrl.Scheme, service)
		break
	case "s3":
		credential := m.Config().Get("credential")
		service, err := aws.NewServiceWithCredential(credential)
		if err != nil {
			return err
		}
		m.service.Register(parsedUrl.Scheme, service)

	default:
		return fmt.Errorf("Unsupported scheme: %v", parsedUrl.Scheme)
	}
	return nil

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
				column, found := mapping["column"]
				if !found {
					column = filed.Name
				}
				values[column] = m.convertIfNeeded(value)
			})
		return values
	}
	return source
}

func getTableURL(manager Manager, table string) string {
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

func (m *FileManager) insertRecord(tableURL string, statement *DmlStatement, parameters toolbox.Iterator) error {

	buf := new(bytes.Buffer)
	reader, err := m.getReaderForUrl(tableURL)
	if reader != nil {
		_, err := io.Copy(buf, reader)
		if err != nil {
			return err
		}
	}

	record, err := m.getRecord(statement, parameters)
	if err != nil {
		return err
	}
	encodedRecord, err := m.encodeRecord(record)
	if err != nil {
		return err
	}

	_, err = buf.Write(([]byte)(encodedRecord))
	if err != nil {
		return err
	}
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
			return fmt.Errorf("Failed to compress data (flush) %v", err)
		}
		err = writer.Close()
		if err != nil {
			return fmt.Errorf("Failed to compress data (close) %v", err)
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
		predicate, err = NewSQLCriteriaPredicate(parameters, statement.Criteria...)
		if err != nil {
			return 0, fmt.Errorf("Failed to read data from %v due to %v", statement.SQL, err)
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
		encodedRecord, err := m.encodeRecord(record)
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
	if m.hasHeaderLine {
		return nil, fmt.Errorf("Modification of delimitered files is not supported yet")
	}
	parser := NewDmlParser()
	statement, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	tableURL := getTableURL(m, statement.Table)

	var count = 0
	parameters := toolbox.NewSliceIterator(sqlParameters)
	switch statement.Type {
	case "INSERT":
		err = m.insertRecord(tableURL, statement, parameters)
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

func (m *FileManager) getReaderForUrl(tableURL string) (io.Reader, error) {
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
		reader, err = gzip.NewReader(reader)
		if err != nil {
			return nil, err
		}
	}
	return reader, nil
}

func (m *FileManager) fetchRecords(table string, predicate toolbox.Predicate, recordHandler func(record map[string]interface{}, matched bool) (bool, error)) error {
	tableURL := getTableURL(m, table)
	reader, err := m.getReaderForUrl(tableURL)
	if reader == nil {
		return err
	}

	scanner := bufio.NewScanner(reader)

	var headers []string
	if m.hasHeaderLine && scanner.Scan() {
		headerLine := scanner.Text()
		for _, column := range strings.Split(headerLine, m.delimiter) {
			headers = append(headers, strings.Trim(column, " "))
		}
	}
	for scanner.Scan() {
		line := scanner.Text()
		decoder := m.decoderFactory.Create(strings.NewReader(line))
		record := make(map[string]interface{})

		if m.hasHeaderLine {
			delimiteredRecord := &DelimiteredRecord{
				Delimiter: m.delimiter,
				Record:    record,
				Columns:   headers,
			}
			err := decoder.Decode(delimiteredRecord)
			if err != nil {
				return fmt.Errorf("Failed to decode record from %v due to %v \n%v\n", table, err, line)
			}
		} else {

			err := decoder.Decode(&record)
			if err != nil {
				return fmt.Errorf("Failed to decode record from %v due to %v \n%v\n", table, err, line)
			}
		}
		matched := true
		if predicate != nil {
			matched = predicate.Apply(record)
		}
		toContinue, err := recordHandler(record, matched)
		if err != nil {
			return fmt.Errorf("Failed to fetch records due to %v", err)
		}
		if !toContinue {
			return nil
		}
	}
	return nil
}

func (m *FileManager) readWithPredicate(connection Connection, statement *QueryStatement, sqlParameters []interface{}, readingHandler func(scanner Scanner) (toContinue bool, err error), predicate toolbox.Predicate) error {
	err := m.fetchRecords(statement.Table, predicate, func(record map[string]interface{}, matched bool) (bool, error) {
		if !matched {
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
		return fmt.Errorf("Failed to parse statement %v, %v", query, err)
	}
	var predicate toolbox.Predicate
	if len(statement.Criteria) > 0 {
		parameters := toolbox.NewSliceIterator(sqlParameters)
		predicate, err = NewSQLCriteriaPredicate(parameters, statement.Criteria...)
		if err != nil {
			return fmt.Errorf("Failed to read data from %v due to %v", query, err)
		}
	}
	return m.readWithPredicate(connection, statement, sqlParameters, readingHandler, predicate)
}

//NewFileManager creates a new file manager.
func NewFileManager(encoderFactory toolbox.EncoderFactory, decoderFactory toolbox.DecoderFactory, valuesDelimiter string) *FileManager {
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
