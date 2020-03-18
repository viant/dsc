package dsc

import (
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

type batch struct {
	processed      int
	tempDir        string
	tempFile       string
	size           int
	sql            string
	writer         *gzip.Writer
	values         []interface{}
	placeholders   string
	columns        string
	dataIndexes    []int
	firstSeq       int64
	bulkInsertType string
	manager        *AbstractManager
	sqlProvider    func(item interface{}) *ParametrizedSQL
	updateId       func(index int, seq int64)
	connection     Connection
	table          string
}

func (b *batch) flush() (int, error) {
	if b.sql == "" {
		return 0, nil
	}

	var dataIndexes = b.dataIndexes
	b.dataIndexes = []int{}
	switch b.bulkInsertType {
	case CopyLocalInsert:
		defer os.Remove(b.tempFile)
		err := b.writer.Flush()
		if err != nil {
			return 0, err
		}
		err = b.writer.Close()
		if err != nil {
			return 0, err
		}
	case BulkInsertAllType:
		b.sql += " SELECT 1 FROM DUAL"
	}
	result, err := b.manager.ExecuteOnConnection(b.connection, b.sql, b.values)
	b.dataIndexes = []int{}
	b.sql = ""
	b.values = []interface{}{}
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	for _, i := range dataIndexes {
		b.firstSeq++
		b.updateId(i, b.firstSeq)
	}
	b.firstSeq = 0
	return int(affected), nil
}

func (b *batch) expandedValues(parametrizedSQL *ParametrizedSQL) string {
	recordLine := b.manager.ExpandSQL(b.placeholders, parametrizedSQL.Values)
	if breakCount := strings.Count(recordLine, "\n"); breakCount > 0 {
		recordLine = strings.Replace(recordLine, "\n", "", breakCount)
	}
	return recordLine + "\n"
}

func (b *batch) transformFirst(parametrizedSQL *ParametrizedSQL) error {
	b.sql = parametrizedSQL.SQL
	b.values = parametrizedSQL.Values
	fragment := " VALUES"
	valuesIndex := strings.Index(parametrizedSQL.SQL, fragment)
	if beginIndex := strings.Index(parametrizedSQL.SQL, "("); beginIndex != -1 {
		names := string(parametrizedSQL.SQL[beginIndex+1:])
		if endIndex := strings.Index(names, ")"); endIndex != -1 {
			b.columns = string(names[:endIndex])
		}
	}
	b.placeholders = strings.Trim(strings.TrimSpace(string(parametrizedSQL.SQL[valuesIndex+7:])), "()")
	switch b.bulkInsertType {
	case CopyLocalInsert:
		b.tempDir = b.manager.config.GetString("tempDir", os.TempDir())
		if b.columns == "" {
			return fmt.Errorf("columns were empty")
		}
		file, err := ioutil.TempFile(b.tempDir, "temp")
		if err != nil {
			return err
		}
		b.tempFile = file.Name()
		b.writer = gzip.NewWriter(file)
		if _, err := b.writer.Write([]byte(b.expandedValues(parametrizedSQL))); err != nil {
			return err
		}

		table := b.table
		b.sql = fmt.Sprintf(`COPY %v(%v)
FROM LOCAL '%v' GZIP
DELIMITER ','
NULL AS 'null'
ENCLOSED BY ''''
`, table, b.columns, file.Name())
		b.values = make([]interface{}, 0)
	case UnionSelectInsert:
		valuesIndex := strings.Index(parametrizedSQL.SQL, " VALUES")
		selectAll := " SELECT " + b.expandedValues(parametrizedSQL)
		selectAll = b.manager.ExpandSQL(selectAll, parametrizedSQL.Values)
		parametrizedSQL.Values = []interface{}{}
		b.sql = b.sql[:valuesIndex] + " " + selectAll

	case BulkInsertAllType:
		b.sql = strings.Replace(b.sql, "INSERT ", "INSERT ALL ", 1)
	default:

	}
	return nil
}

func (b *batch) transformNext(parametrizedSQL *ParametrizedSQL) error {
	switch b.bulkInsertType {
	case CopyLocalInsert:
		_, err := b.writer.Write([]byte(b.expandedValues(parametrizedSQL)))
		return err
	case UnionSelectInsert:
		b.sql += "\nUNION ALL SELECT " + b.expandedValues(parametrizedSQL)
	case BulkInsertAllType:
		b.sql += fmt.Sprintf("\nINTO %v(%v) VALUES(%v)", b.table, b.columns, b.placeholders)
		b.values = append(b.values, parametrizedSQL.Values...)
	default:
		b.sql += fmt.Sprintf(",(%v)", b.placeholders)
		b.values = append(b.values, parametrizedSQL.Values...)
	}
	return nil
}

func (b *batch) persist(index int, item interface{}) error {
	parametrizedSQL := b.sqlProvider(item)
	if len(parametrizedSQL.Values) == 1 && parametrizedSQL.Type == SQLTypeUpdate {
		//nothing to udpate, one parameter is ID=? without values to update
		return nil
	}
	if parametrizedSQL.Type == SQLTypeInsert && b.size > 0 {
		if len(b.dataIndexes) > b.size {
			if _, err := b.flush(); err != nil {
				return err
			}
		}
		b.dataIndexes = append(b.dataIndexes, index)
		if isFirst := len(b.sql) == 0; isFirst {
			return b.transformFirst(parametrizedSQL)
		}
		return b.transformNext(parametrizedSQL)
	}
	result, err := b.manager.ExecuteOnConnection(b.connection, parametrizedSQL.SQL, parametrizedSQL.Values)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	b.processed += int(affected)
	seq, _ := result.LastInsertId()
	if b.size > 0 && b.firstSeq == 0 {
		b.firstSeq = seq
	}
	b.updateId(index, seq)
	return nil
}

func newBatch(table string, connection Connection, manager *AbstractManager, sqlProvider func(item interface{}) *ParametrizedSQL, updateId func(index int, seq int64)) *batch {
	dialect := GetDatastoreDialect(manager.Config().DriverName)
	var batchSize = manager.Config().GetInt(BatchSizeKey, defaultBatchSize)
	Logf("batch size: %v\n", batchSize)
	canUseBatch := dialect != nil && dialect.CanPersistBatch() && batchSize > 1
	if !canUseBatch {
		batchSize = 0
	}
	insertType := ""
	if dialect != nil {
		insertType = dialect.BulkInsertType()
	}
	return &batch{
		connection:     connection,
		updateId:       updateId,
		sqlProvider:    sqlProvider,
		size:           batchSize,
		values:         []interface{}{},
		dataIndexes:    []int{},
		bulkInsertType: insertType,
		manager:        manager,
		table:          table,
	}
}
