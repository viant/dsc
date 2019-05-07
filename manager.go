package dsc

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/viant/toolbox"
)

var defaultBatchSize = 512

var BulkInsertAllType = "insertAll"

//AbstractManager represent general abstraction for datastore implementation.
// Note that ExecuteOnConnection,  ReadAllOnWithHandlerOnConnection may need to be implemented for particular datastore.
type AbstractManager struct {
	Manager
	config                  *Config
	connectionProvider      ConnectionProvider
	tableDescriptorRegistry TableDescriptorRegistry
	limiter                 *Limiter
}

//Config returns a config.
func (m *AbstractManager) Config() *Config {
	return m.config
}

//ConnectionProvider returns a connection provider.
func (m *AbstractManager) ConnectionProvider() ConnectionProvider {
	return m.connectionProvider
}

//Execute executes passed in sql with parameters.  It returns sql result, or an error.
func (m *AbstractManager) Execute(sql string, sqlParameters ...interface{}) (result sql.Result, err error) {
	var connection Connection
	connection, err = m.Manager.ConnectionProvider().Get()
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return m.Manager.ExecuteOnConnection(connection, sql, sqlParameters)
}

//ExecuteAll passed in SQL. It returns sql result, or an error.
func (m *AbstractManager) ExecuteAll(sqls []string) ([]sql.Result, error) {
	connection, err := m.Manager.ConnectionProvider().Get()
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return m.Manager.ExecuteAllOnConnection(connection, sqls)
}

//Acquire if max request per second is specified this function will throttle any request exceeding specified max
func (m *AbstractManager) Acquire() {
	if m.config.MaxRequestPerSecond == 0 {
		return
	}

	m.limiter.Acquire()
}

//ExecuteAllOnConnection executes passed in SQL on connection. It returns sql result, or an error.
func (m *AbstractManager) ExecuteAllOnConnection(connection Connection, sqls []string) ([]sql.Result, error) {
	var result = make([]sql.Result, len(sqls))

	err := connection.Begin()
	if err != nil {
		return nil, err
	}
	defer func() {
		if err == nil {
			connection.Commit()
		}
		if err != nil {
			connection.Rollback()
		}
	}()
	for i, sql := range sqls {
		var err error
		result[i], err = m.Manager.ExecuteOnConnection(connection, sql, nil)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

//ReadAllWithHandler executes query with parameters and for each fetch row call reading handler with a scanner, to continue reading next row, scanner needs to return true.
func (m *AbstractManager) ReadAllWithHandler(query string, queryParameters []interface{}, readingHandler func(scanner Scanner) (toContinue bool, err error)) error {
	connection, err := m.Manager.ConnectionProvider().Get()
	if err != nil {
		return err
	}
	defer connection.Close()
	return m.Manager.ReadAllOnWithHandlerOnConnection(connection, query, queryParameters, readingHandler)
}

//ReadAll executes query with parameters and fetches all table rows. The row is mapped to result slice pointer with record mapper.
func (m AbstractManager) ReadAll(resultSlicePointer interface{}, query string, queryParameters []interface{}, mapper RecordMapper) error {
	connection, err := m.Manager.ConnectionProvider().Get()
	if err != nil {
		return err
	}
	defer connection.Close()

	return m.Manager.ReadAllOnConnection(connection, resultSlicePointer, query, queryParameters, mapper)
}

//ReadAllOnConnection executes query with parameters on passed in connection and fetches all table rows. The row is mapped to result slice pointer with record mapper.
func (m *AbstractManager) ReadAllOnConnection(connection Connection, resultSlicePointer interface{}, query string, queryParameters []interface{}, mapper RecordMapper) error {
	toolbox.AssertPointerKind(resultSlicePointer, reflect.Slice, "resultSlicePointer")
	slice := reflect.ValueOf(resultSlicePointer).Elem()
	if mapper == nil {
		mapper = NewRecordMapperIfNeeded(mapper, reflect.TypeOf(resultSlicePointer).Elem().Elem())
	}
	err := m.Manager.ReadAllOnWithHandlerOnConnection(connection, query, queryParameters, func(scannalbe Scanner) (toContinue bool, err error) {
		mapped, providerError := mapper.Map(scannalbe)
		if providerError != nil {
			return false, fmt.Errorf("failed to map row sql: %v  due to %v", query, providerError.Error())
		}
		if mapped != nil {
			//only add to slice i
			mappedValue := reflect.ValueOf(mapped)
			if reflect.TypeOf(resultSlicePointer).Elem().Kind() == reflect.Slice && reflect.TypeOf(resultSlicePointer).Elem().Elem().Kind() != mappedValue.Kind() {
				if mappedValue.Kind() == reflect.Ptr {
					mappedValue = mappedValue.Elem()
				}
			}
			slice.Set(reflect.Append(slice, mappedValue))
		}
		return true, nil
	})
	return err
}

//ReadSingle executes query with parameters and reads on connection single table row. The row is mapped to result pointer with record mapper.
func (m *AbstractManager) ReadSingle(resultPointer interface{}, query string, queryParameters []interface{}, mapper RecordMapper) (success bool, err error) {
	connection, err := m.Manager.ConnectionProvider().Get()
	if err != nil {
		return false, err
	}
	defer connection.Close()
	return m.Manager.ReadSingleOnConnection(connection, resultPointer, query, queryParameters, mapper)
}

//ReadSingleOnConnection executes query with parameters on passed in connection and reads single table row. The row is mapped to result pointer with record mapper.
func (m *AbstractManager) ReadSingleOnConnection(connection Connection, resultPointer interface{}, query string, queryParameters []interface{}, mapper RecordMapper) (success bool, err error) {
	toolbox.AssertKind(resultPointer, reflect.Ptr, "resultStruct")
	if mapper == nil {
		mapper = NewRecordMapperIfNeeded(mapper, reflect.TypeOf(resultPointer).Elem())
	}
	var mapped interface{}
	var elementType = reflect.TypeOf(resultPointer).Elem()
	err = m.Manager.ReadAllOnWithHandlerOnConnection(connection, query, queryParameters, func(scanner Scanner) (toContinue bool, err error) {
		mapped, err = mapper.Map(scanner)
		if err != nil {
			return false, fmt.Errorf("failed to map record: %v with %T due to %v", query, mapper, err)
		}
		if mapped != nil {
			if elementType.Kind() == reflect.Slice {
				slice := reflect.ValueOf(resultPointer).Elem()
				toolbox.ProcessSlice(mapped, func(item interface{}) bool {
					if item == nil {
						slice.Set(reflect.Append(slice, reflect.Zero(elementType)))
						return true
					}
					slice.Set(reflect.Append(slice, reflect.ValueOf(item)))
					return true
				})

			} else if elementType.Kind() == reflect.Map {
				if err = toolbox.ProcessMap(mapped, func(key, value interface{}) bool {
					aMap := reflect.ValueOf(resultPointer).Elem()
					aMap.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(value))
					return true
				}); err != nil {
					return false, err
				}
			} else {
				if reflect.ValueOf(mapped).Kind() == reflect.Ptr {
					mapped = reflect.ValueOf(mapped).Elem().Interface()
				}
				reflect.ValueOf(resultPointer).Elem().Set(reflect.ValueOf(mapped))
			}
			success = true
		}

		return false, nil
	})
	return success, err
}

//PersistAll persists all table rows, dmlProvider is used to generate insert or update statement. It returns number of inserted, updated or error.
//If driver allows this operation is executed in one transaction.
func (m *AbstractManager) PersistAll(dataPointer interface{}, table string, provider DmlProvider) (int, int, error) {
	connection, err := m.Manager.ConnectionProvider().Get()
	if err != nil {
		return 0, 0, err
	}
	defer connection.Close()

	err = connection.Begin()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to start transaction on %v due to %v", m.config.Descriptor, err)
	}
	inserted, updated, err := m.Manager.PersistAllOnConnection(connection, dataPointer, table, provider)
	if err == nil {
		commitErr := connection.Commit()
		if commitErr != nil {
			return 0, 0, fmt.Errorf("failed to commit on %v due to %v", m.config.Descriptor, commitErr)
		}
	} else {
		rollbackErr := connection.Rollback()
		if rollbackErr != nil {
			return 0, 0, fmt.Errorf("failed to rollback on %v due to %v, %v", m.config.Descriptor, err, rollbackErr)
		}
	}
	return inserted, updated, err
}

//RegisterDescriptorIfNeeded register a table descriptor if there it is not present, returns a pointer to a table descriptor.
func (m *AbstractManager) RegisterDescriptorIfNeeded(table string, instance interface{}) (*TableDescriptor, error) {
	if !m.tableDescriptorRegistry.Has(table) {
		descriptor, err := NewTableDescriptor(table, instance)
		if err != nil {
			return nil, err
		}
		_ = m.tableDescriptorRegistry.Register(descriptor)
	}
	var result = m.tableDescriptorRegistry.Get(table)
	if result != nil {
		return result, nil
	}
	return nil, fmt.Errorf("failed to lookup descriptor for table: %v", table)
}

//PersistAllOnConnection persists on connection all table rows, dmlProvider is used to generate insert or update statement. It returns number of inserted, updated or error.
func (m *AbstractManager) PersistAllOnConnection(connection Connection, dataPointer interface{}, table string, provider DmlProvider) (inserted int, updated int, err error) {

	if ranger, isRanger := dataPointer.(toolbox.Ranger); isRanger {
		collection := toolbox.AsSlice(ranger)
		dataPointer = &collection
	} else if iterator, isRanger := dataPointer.(toolbox.Iterator); isRanger {
		collection := toolbox.AsSlice(iterator)
		dataPointer = &collection
	}

	toolbox.AssertPointerKind(dataPointer, reflect.Slice, "resultSlicePointer")
	structType := reflect.TypeOf(dataPointer).Elem().Elem()
	provider, err = NewDmlProviderIfNeeded(provider, table, structType)
	if err != nil {
		return 0, 0, err
	}
	descriptor, err := m.RegisterDescriptorIfNeeded(table, dataPointer)
	if err != nil {
		return 0, 0, err
	}
	insertables, updatables, err := m.Manager.ClassifyDataAsInsertableOrUpdatable(connection, dataPointer, table, provider)
	if err != nil {
		return 0, 0, err
	}

	var isStructPointer = structType.Kind() == reflect.Ptr
	var insertableMapping map[int]int
	if descriptor.Autoincrement {
		//we need to store original position of item, vs insertables, to set back autoincrement changed item to original slice
		insertableMapping = make(map[int]int)

		toolbox.ProcessSliceWithIndex(dataPointer, func(index int, value interface{}) bool {
			for j, insertable := range insertables {
				if isStructPointer {
					if insertable == value {
						insertableMapping[j] = index
						break
					}
				} else {
					if reflect.DeepEqual(insertable, value) {
						insertableMapping[j] = index
						break
					}
				}
			}
			return true
		})
	}

	inserted, insertErr := m.Manager.PersistData(connection, insertables, table, provider, func(item interface{}) *ParametrizedSQL {
		return provider.Get(SQLTypeInsert, item)
	})

	if insertErr != nil {
		return 0, 0, insertErr
	}
	if descriptor.Autoincrement {
		for k, v := range insertableMapping {
			value := insertables[k]
			toolbox.SetSliceValue(dataPointer, v, value)
		}
	}

	updated, updateErr := m.Manager.PersistData(connection, updatables, table, provider, func(item interface{}) *ParametrizedSQL {
		return provider.Get(SQLTypeUpdate, item)
	})

	if updateErr != nil {
		return 0, 0, updateErr
	}

	return inserted, updated, nil
}

//PersistSingle persists single table row, dmlProvider is used to generate insert or update statement. It returns number of inserted, updated or error.
func (m *AbstractManager) PersistSingle(dataPointer interface{}, table string, provider DmlProvider) (inserted int, updated int, err error) {
	slice := convertToTypesSlice(dataPointer)
	inserted, updated, err = m.Manager.PersistAll(slice, table, provider)
	if err != nil {
		return 0, 0, err
	}
	if inserted > 0 {
		descriptor, err := m.RegisterDescriptorIfNeeded(table, dataPointer)
		if err != nil {
			return 0, 0, err
		}
		if descriptor.Autoincrement {
			value := toolbox.GetSliceValue(slice, 0)
			reflect.ValueOf(dataPointer).Elem().Set(reflect.ValueOf(value))
		}
	}
	return inserted, updated, err
}

//PersistSingleOnConnection persists on connection single table row, dmlProvider is used to generate insert or udpate statement. It returns number of inserted, updated or error.
func (m *AbstractManager) PersistSingleOnConnection(connection Connection, dataPointer interface{}, table string, provider DmlProvider) (inserted int, updated int, err error) {
	slice := []interface{}{dataPointer}
	return m.Manager.PersistAllOnConnection(connection, &slice, table, provider)
}

type batchControl struct {
	sql         string
	values      []interface{}
	dataIndexes []int
	firstSeq    int64
	isInsertAll bool
	manager     Manager
}

func (c *batchControl) Flush(connection Connection, updateId func(index int, seq int64)) (int, error) {
	if c.sql == "" {
		return 0, nil
	}
	var dataIndexes = c.dataIndexes
	c.dataIndexes = []int{}
	if c.isInsertAll {
		c.sql += " SELECT 1 FROM DUAL"
	}
	result, err := c.manager.ExecuteOnConnection(connection, c.sql, c.values)
	c.sql = ""
	c.values = []interface{}{}
	if err != nil {
		return 0, err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	for _, i := range dataIndexes {
		c.firstSeq++
		updateId(i, c.firstSeq)
	}
	c.firstSeq = 0
	return int(affected), nil
}

//PersistData persist data on connection on table, keySetter is used to optionally set autoincrement column, sqlProvider handler will generate ParametrizedSQL with Insert or Update statement.
func (m *AbstractManager) PersistData(connection Connection, data interface{}, table string, keySetter KeySetter, sqlProvider func(item interface{}) *ParametrizedSQL) (int, error) {
	var processed = 0
	dialect := GetDatastoreDialect(m.config.DriverName)

	var batchSize = m.config.GetInt(BatchSizeKey, defaultBatchSize)
	Logf("batch size: %v\n", batchSize)

	canUseBatch := dialect != nil && dialect.CanPersistBatch() && batchSize > 1

	isInsertAll := dialect.BulkInsertType() == BulkInsertAllType

	Logf("[%v]: canUseBatch: %v\n", m.config.DriverName, canUseBatch)

	//TODO may need to move batch insert to dialect ?

	var batchControl = &batchControl{
		values:      []interface{}{},
		dataIndexes: []int{},
		manager:     m.Manager,
		isInsertAll: isInsertAll,
	}

	var collection = make([]interface{}, 0)
	updateId := func(index int, seq int64) {
		if seq == 0 || index < 0 {
			return
		}
		var ptrType = false
		dataType := reflect.TypeOf(collection[index])
		itemValue := reflect.ValueOf(collection[index])
		if dataType.Kind() == reflect.Ptr {
			dataType = dataType.Elem()
			ptrType = true
		}
		if itemValue.Kind() == reflect.Ptr {
			itemValue = itemValue.Elem()
			ptrType = true
		}
		structPointerValue := reflect.New(dataType)
		reflect.Indirect(structPointerValue).Set(itemValue)

		if keySetter != nil {
			keySetter.SetKey(structPointerValue.Interface(), seq)
		}
		if ptrType {
			collection[index] = structPointerValue.Interface()
		} else {
			collection[index] = structPointerValue.Elem().Interface()
		}

	}

	persist := func(index int, item interface{}) error {
		parametrizedSQL := sqlProvider(item)
		if len(parametrizedSQL.Values) == 1 && parametrizedSQL.Type == SQLTypeUpdate {
			//nothing to udpate, one parameter is ID=? without values to update
			return nil
		}

		if parametrizedSQL.Type == SQLTypeInsert && canUseBatch {
			if len(batchControl.dataIndexes) > batchSize {
				if _, err := batchControl.Flush(connection, updateId); err != nil {
					return err
				}
			}
			batchControl.dataIndexes = append(batchControl.dataIndexes, index)
			if len(batchControl.sql) == 0 {
				batchControl.sql = parametrizedSQL.SQL
				if isInsertAll {
					batchControl.sql = strings.Replace(batchControl.sql, "INSERT ", "INSERT ALL ", 1)
				}
				batchControl.values = parametrizedSQL.Values
				return nil
			}

			fragment := " VALUES"
			if isInsertAll {
				fragment = "INSERT "
			}
			valuesIndex := strings.Index(parametrizedSQL.SQL, fragment)
			if valuesIndex != -1 {
				if isInsertAll {
					batchControl.sql += "\n" + string(parametrizedSQL.SQL[valuesIndex+7:])
				} else {
					batchControl.sql += "," + string(parametrizedSQL.SQL[valuesIndex+7:])
				}
				batchControl.values = append(batchControl.values, parametrizedSQL.Values...)
			}
			return nil
		}

		result, err := m.Manager.ExecuteOnConnection(connection, parametrizedSQL.SQL, parametrizedSQL.Values)
		if err != nil {
			return err
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}

		processed += int(affected)
		seq, _ := result.LastInsertId()
		if canUseBatch && batchControl.firstSeq == 0 {
			batchControl.firstSeq = seq
		}
		updateId(index, seq)
		return nil
	}

	var err error
	if ranger, ok := data.(toolbox.Ranger); ok {
		err = ranger.Range(func(item interface{}) (b bool, e error) {
			err = persist(-1, item)
			return err == nil, err
		})
	} else if iterator, ok := data.(toolbox.Iterator); ok {
		for iterator.HasNext() && err == nil {
			var item interface{}
			if err = iterator.Next(&item); err == nil {
				err = persist(-1, item)
			}
		}
	} else if toolbox.IsSlice(data) {
		collection = toolbox.AsSlice(data)
		for i, item := range collection {
			if err = persist(i, item); err != nil {
				break
			}
		}
	}
	if batchControl != nil && err == nil {
		if _, err := batchControl.Flush(connection, updateId); err != nil {
			return 0, err
		}
	}
	return processed, err
}

func (m *AbstractManager) fetchDataInBatches(connection Connection, sqlsWihtArguments []*ParametrizedSQL, mapper RecordMapper) (*[][]interface{}, error) {
	var rows = make([][]interface{}, 0)
	for _, sqlWihtArguments := range sqlsWihtArguments {
		if len(sqlWihtArguments.Values) == 0 {
			break
		}
		err := m.Manager.ReadAllOnConnection(connection, &rows, sqlWihtArguments.SQL, sqlWihtArguments.Values, mapper)
		if err != nil {
			return nil, err
		}
	}
	return &rows, nil
}

func (m *AbstractManager) fetchExistingData(connection Connection, table string, pkValues [][]interface{}, provider DmlProvider) ([][]interface{}, error) {
	var rows = make([][]interface{}, 0)
	descriptor := m.tableDescriptorRegistry.Get(table)

	if len(pkValues) > 0 {
		descriptor := TableDescriptor{Table: table, PkColumns: descriptor.PkColumns}
		sqlBuilder := NewQueryBuilder(&descriptor, "")
		sqlWithArguments := sqlBuilder.BuildBatchedQueryOnPk(descriptor.PkColumns, pkValues, defaultBatchSize)

		var mapper = NewColumnarRecordMapper(false, reflect.TypeOf(rows))
		batched, err := m.fetchDataInBatches(connection, sqlWithArguments, mapper)
		if err != nil {
			return nil, err
		}
		rows = append(rows, (*batched)...)
	}
	return rows, nil
}

//ClassifyDataAsInsertableOrUpdatable classifies passed in data as insertable or updatable.
func (m *AbstractManager) ClassifyDataAsInsertableOrUpdatable(connection Connection, dataPointer interface{}, table string, provider DmlProvider) ([]interface{}, []interface{}, error) {
	if provider == nil {
		return nil, nil, errors.New("provider was nil")
	}

	var rowsByKey = make(map[string]interface{}, 0)
	var candidates, insertables, updatables = make([]interface{}, 0), make([]interface{}, 0), make([]interface{}, 0)
	var pkValues = make([][]interface{}, 0)

	hasPK := len(m.tableDescriptorRegistry.Get(table).PkColumns) > 0
	toolbox.ProcessSlice(dataPointer, func(row interface{}) bool {
		var pkValueForThisRow = provider.Key(row)
		for _, v := range pkValueForThisRow {
			if v == nil { //if pk value null, this row has to be insertable
				insertables = append(insertables, row)
				return true
			}
		}
		candidates = append(candidates, row)
		key := toolbox.JoinAsString(pkValueForThisRow, "")
		pkValues = append(pkValues, pkValueForThisRow)
		rowsByKey[key] = row
		return true
	})

	if hasPK { //only if has PK, otherwise always insert
		//fetch all existing pk values into rows to classify as updatable
		rows, err := m.fetchExistingData(connection, table, pkValues, provider)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to fetch existing data: due to:\n\t%v", err.Error())
		}
		//process existing rows and add mapped entires as updatables
		for _, row := range rows {
			key := toolbox.JoinAsString(row, "")
			if instance, ok := rowsByKey[key]; ok {
				updatables = append(updatables, instance)
				delete(rowsByKey, key)
			}
		}
	}

	//go over all candidates and if no key or entries still found in rows by key then classify as insertable
	for _, candidate := range candidates {
		var values = provider.Key(candidate)
		key := toolbox.JoinAsString(values, "")
		if _, ok := rowsByKey[key]; ok {
			insertables = append(insertables, candidate)
		}
	}
	return insertables, updatables, nil
}

//DeleteAll deletes all rows for passed in table,  key provider is used to extract primary keys. It returns number of deleted rows or error.
func (m *AbstractManager) DeleteAll(dataPointer interface{}, table string, keyProvider KeyGetter) (deleted int, err error) {
	connection, err := m.Manager.ConnectionProvider().Get()
	if err != nil {
		return 0, err
	}
	defer connection.Close()
	err = connection.Begin()
	if err != nil {
		return 0, fmt.Errorf("failed to start transaction on %v due to %v", m.config.Descriptor, err)
	}
	deleted, err = m.DeleteAllOnConnection(connection, dataPointer, table, keyProvider)
	if err == nil {
		commitErr := connection.Commit()
		if commitErr != nil {
			return 0, fmt.Errorf("failed to commit on %v due to %v", m.config.Descriptor, commitErr)
		}
	} else {
		rollbackErr := connection.Rollback()
		if rollbackErr != nil {
			return 0, fmt.Errorf("failed to rollback on %v due to %v, %v", m.config.Descriptor, err, rollbackErr)
		}
	}
	return deleted, err
}

//DeleteAllOnConnection deletes all rows on connection from table, key provider is used to extract primary keys. It returns number of deleted rows or error.
//If driver allows this operation is executed in one transaction.
func (m *AbstractManager) DeleteAllOnConnection(connection Connection, dataPointer interface{}, table string, keyProvider KeyGetter) (deleted int, err error) {

	deleted = 0
	structType := toolbox.DiscoverTypeByKind(dataPointer, reflect.Struct)
	keyProvider, err = NewKeyGetterIfNeeded(keyProvider, table, structType)
	if err != nil {
		return 0, err
	}
	m.RegisterDescriptorIfNeeded(table, dataPointer)

	descriptor := m.tableDescriptorRegistry.Get(table)
	toolbox.ProcessSlice(dataPointer, func(item interface{}) bool {
		if err != nil {
			return false
		}
		where := strings.Join(descriptor.PkColumns, ",")
		if len(descriptor.PkColumns) > 1 {
			values := strings.Repeat("?,", len(descriptor.PkColumns))
			values = values[0 : len(values)-1]
			where = where + " IN (" + values + ")"
		} else {
			where = where + " = ?"
		}
		dml := fmt.Sprintf(deleteSQLTemplate, table, where)
		var result sql.Result
		result, err = m.Manager.ExecuteOnConnection(connection, dml, keyProvider.Key(item))
		if err != nil {
			return false
		}
		var affected int64
		affected, err = result.RowsAffected()
		if err == nil {
			deleted = deleted + int(affected)
		}
		return true
	})
	if err != nil {
		return 0, err
	}
	return deleted, nil
}

//DeleteSingle deletes single row from table on for passed in data pointer, key provider is used to extract primary keys. It returns boolean if successful, or error.
func (m *AbstractManager) DeleteSingle(dataPointer interface{}, table string, keyProvider KeyGetter) (bool, error) {
	connection, err := m.Manager.ConnectionProvider().Get()
	if err != nil {
		return false, err
	}
	defer connection.Close()
	err = connection.Begin()
	if err != nil {
		return false, fmt.Errorf("failed to start transaction on %v due to %v", m.config.Descriptor, err)
	}
	suceess, err := m.DeleteSingleOnConnection(connection, dataPointer, table, keyProvider)
	if err == nil {
		commitErr := connection.Commit()
		if commitErr != nil {
			return false, fmt.Errorf("failed to commit on %v due to %v", m.config.Descriptor, commitErr)
		}
	} else {
		rollbackErr := connection.Rollback()
		if rollbackErr != nil {
			return false, fmt.Errorf("failed to rollback on %v due to %v, %v", m.config.Descriptor, err, rollbackErr)
		}
	}
	return suceess, err
}

func convertToTypesSlice(dataPointer interface{}) interface{} {
	toolbox.AssertPointerKind(dataPointer, reflect.Struct, "slicePointer")
	sliceValue := reflect.ValueOf(toolbox.DereferenceValue(dataPointer))
	sliceType := reflect.SliceOf(sliceValue.Type())
	slicePointer := reflect.New(sliceType)
	slice := slicePointer.Elem()
	slice.Set(reflect.Append(slice, sliceValue))
	return slicePointer.Interface()
}

//DeleteSingleOnConnection deletes data on connection from table on for passed in data pointer, key provider is used to extract primary keys. It returns true if successful.
func (m *AbstractManager) DeleteSingleOnConnection(connection Connection, dataPointer interface{}, table string, keyProvider KeyGetter) (bool, error) {
	toolbox.AssertPointerKind(dataPointer, reflect.Struct, "dataPointer")
	slice := convertToTypesSlice(dataPointer)
	deleted, err := m.Manager.DeleteAllOnConnection(connection, slice, table, keyProvider)
	if err != nil {
		return false, err
	}
	return deleted == 1, nil
}

//ExpandSQL expands sql with passed in arguments
func (m *AbstractManager) ExpandSQL(sql string, arguments []interface{}) string {
	for _, arg := range arguments {
		stringArg := toolbox.AsString(arg)
		if arg == nil {
			stringArg = "NULL"
		} else {
			if toolbox.IsString(arg) || toolbox.CanConvertToString(arg) {
				stringArg = "'" + stringArg + "'"
			}
		}
		sql = strings.Replace(sql, "?", stringArg, 1)
	}
	return sql
}

//TableDescriptorRegistry returns a table descriptor registry
func (m *AbstractManager) TableDescriptorRegistry() TableDescriptorRegistry {
	return m.tableDescriptorRegistry
}

//NewAbstractManager create a new abstract manager, it takes config, conneciton provider, and target (sub class) manager
func NewAbstractManager(config *Config, connectionProvider ConnectionProvider, self Manager) *AbstractManager {
	var descriptorRegistry = newTableDescriptorRegistry()
	var result = &AbstractManager{config: config, connectionProvider: connectionProvider, Manager: self, tableDescriptorRegistry: descriptorRegistry}
	descriptorRegistry.manager = result
	if config.MaxRequestPerSecond > 0 {
		result.limiter = NewLimiter(time.Second, config.MaxRequestPerSecond)
	}
	return result
}
