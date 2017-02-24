package dsc

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/toolbox"
)

var batchSize = 200

//AbstractManager represent general abstraction for datastore implementation.
// Note that ExecuteOnConnection,  ReadAllOnWithHandlerOnConnection may need to be implemented for particular datastore.
type AbstractManager struct {
	Manager
	config                  *Config
	connectionProvider      ConnectionProvider
	tableDescriptorRegistry TableDescriptorRegistry
}

//Config returns a config.
func (am *AbstractManager) Config() *Config {
	return am.config
}

//ConnectionProvider returns a connection provider.
func (am *AbstractManager) ConnectionProvider() ConnectionProvider {
	return am.connectionProvider
}

//Execute executes passed in sql with parameters.  It returns sql result, or an error.
func (am *AbstractManager) Execute(sql string, sqlParameters ...interface{}) (result sql.Result, err error) {
	var connection Connection
	connection, err = am.Manager.ConnectionProvider().Get()
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return am.Manager.ExecuteOnConnection(connection, sql, sqlParameters)
}

//ExecuteAll passed in SQLs. It returns sql result, or an error.
func (am *AbstractManager) ExecuteAll(sqls []string) ([]sql.Result, error) {
	connection, err := am.Manager.ConnectionProvider().Get()
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	return am.Manager.ExecuteAllOnConnection(connection, sqls)
}

//ExecuteAllOnConnection executes passed in SQLs on connection. It returns sql result, or an error.
func (am *AbstractManager) ExecuteAllOnConnection(connection Connection, sqls []string) ([]sql.Result, error) {
	var result = make([]sql.Result, len(sqls))
	for i, sql := range sqls {
		var err error
		result[i], err = am.Manager.ExecuteOnConnection(connection, sql, nil)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

//ReadAllWithHandler executes query with parameters and for each fetch row call reading handler with a scanner, to continue reading next row, scanner needs to return true.
func (am *AbstractManager) ReadAllWithHandler(query string, queryParameters []interface{}, readingHandler func(scanner Scanner) (toContinue bool, err error)) error {
	connection, err := am.Manager.ConnectionProvider().Get()
	if err != nil {
		return err
	}
	defer connection.Close()
	return am.Manager.ReadAllOnWithHandlerOnConnection(connection, query, queryParameters, readingHandler)
}

//ReadAll executes query with parameters and fetches all table rows. The row is mapped to result slice pointer with record mapper.
func (am AbstractManager) ReadAll(resultSlicePointer interface{}, query string, queryParameters []interface{}, mapper RecordMapper) error {
	connection, err := am.Manager.ConnectionProvider().Get()
	if err != nil {
		return err
	}
	defer connection.Close()

	return am.Manager.ReadAllOnConnection(connection, resultSlicePointer, query, queryParameters, mapper)
}

//ReadAllOnConnection executes query with parameters on passed in connection and fetches all table rows. The row is mapped to result slice pointer with record mapper.
func (am *AbstractManager) ReadAllOnConnection(connection Connection, resultSlicePointer interface{}, query string, queryParameters []interface{}, mapper RecordMapper) error {
	toolbox.AssertPointerKind(resultSlicePointer, reflect.Slice, "resultSlicePointer")
	slice := reflect.ValueOf(resultSlicePointer).Elem()
	if mapper == nil {
		mapper = NewRecordMapperIfNeeded(mapper, reflect.TypeOf(resultSlicePointer).Elem().Elem())
	}
	err := am.Manager.ReadAllOnWithHandlerOnConnection(connection, query, queryParameters, func(scannalbe Scanner) (toContinue bool, err error) {
		mapped, providerError := mapper.Map(scannalbe)
		if providerError != nil {
			return false, fmt.Errorf("Failed to map row sql: %v  due to %v", query, providerError.Error())
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
func (am *AbstractManager) ReadSingle(resultPointer interface{}, query string, queryParameters []interface{}, mapper RecordMapper) (success bool, err error) {
	connection, err := am.Manager.ConnectionProvider().Get()
	if err != nil {
		return false, err
	}
	defer connection.Close()
	return am.Manager.ReadSingleOnConnection(connection, resultPointer, query, queryParameters, mapper)
}

//ReadSingleOnConnection executes query with parameters on passed in connection and reads single table row. The row is mapped to result pointer with record mapper.
func (am *AbstractManager) ReadSingleOnConnection(connection Connection, resultPointer interface{}, query string, queryParameters []interface{}, mapper RecordMapper) (success bool, err error) {
	toolbox.AssertKind(resultPointer, reflect.Ptr, "resultStruct")
	if mapper == nil {
		mapper = NewRecordMapperIfNeeded(mapper, reflect.TypeOf(resultPointer).Elem())
	}

	var mapped interface{}
	var elementType = reflect.TypeOf(resultPointer).Elem()

	err = am.Manager.ReadAllOnWithHandlerOnConnection(connection, query, queryParameters, func(scanner Scanner) (toContinue bool, err error) {
		mapped, err = mapper.Map(scanner)
		if err != nil {
			return false, fmt.Errorf("Failed to map record sql: %v due to %v", query, err)
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
func (am *AbstractManager) PersistAll(dataPoiner interface{}, table string, provider DmlProvider) (int, int, error) {
	connection, err := am.Manager.ConnectionProvider().Get()
	if err != nil {
		return 0, 0, err
	}
	if err != nil {
		return 0, 0, err
	}
	defer connection.Close()

	err = connection.Begin()
	if err != nil {
		return 0, 0, fmt.Errorf("Failed to start transaction on %v due to %v", am.config.Descriptor, err)
	}
	inserted, updated, err := am.Manager.PersistAllOnConnection(connection, dataPoiner, table, provider)
	if err == nil {
		commitErr := connection.Commit()
		if commitErr != nil {
			return 0, 0, fmt.Errorf("Failed to commit on %v due to %v", am.config.Descriptor, commitErr)
		}
	} else {
		rollbackErr := connection.Rollback()
		if rollbackErr != nil {
			return 0, 0, fmt.Errorf("Failed to rollback on %v due to %v, %v", am.config.Descriptor, err, rollbackErr)
		}
	}
	return inserted, updated, err
}

//RegisterDescriptorIfNeeded register a table descriptor if there it is not present, returns a pointer to a table descriptor.
func (am *AbstractManager) RegisterDescriptorIfNeeded(table string, instance interface{}) *TableDescriptor {
	if !am.tableDescriptorRegistry.Has(table) {
		descriptor := NewTableDescriptor(table, instance)
		am.tableDescriptorRegistry.Register(descriptor)
	}
	return am.tableDescriptorRegistry.Get(table)
}

//PersistAllOnConnection persists on connection all table rows, dmlProvider is used to generate insert or update statement. It returns number of inserted, updated or error.
func (am *AbstractManager) PersistAllOnConnection(connection Connection, dataPointer interface{}, table string, provider DmlProvider) (inserted int, updated int, err error) {
	toolbox.AssertPointerKind(dataPointer, reflect.Slice, "resultSlicePointer")
	structType := reflect.TypeOf(dataPointer).Elem().Elem()
	provider = NewDmlProviderIfNeeded(provider, table, structType)
	descriptor := am.RegisterDescriptorIfNeeded(table, dataPointer)
	insertables, updatables, err := am.Manager.ClassifyDataAsInsertableOrUpdatable(connection, dataPointer, table, provider)
	if err != nil {
		return 0, 0, err
	}

	var insertableMapping map[int]int
	if descriptor.Autoincrement { //we need to store original position of item, vs insertables, to set back autoincrement changed item to original slice
		insertableMapping = make(map[int]int)
		toolbox.ProcessSliceWithIndex(dataPointer, func(index int, value interface{}) bool {
			for j, insertable := range insertables {
				if reflect.DeepEqual(insertable, value) {
					insertableMapping[j] = index
					break
				}
			}
			return true
		})
	}

	inserted, insertErr := am.Manager.PersistData(connection, insertables, table, provider, func(item interface{}) *ParametrizedSQL {
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

	updated, updateErr := am.Manager.PersistData(connection, updatables, table, provider, func(item interface{}) *ParametrizedSQL {
		return provider.Get(SQLTypeUpdate, item)
	})

	if updateErr != nil {
		return 0, 0, updateErr
	}

	return inserted, updated, nil
}

//PersistSingle persists single table row, dmlProvider is used to generate insert or update statement. It returns number of inserted, updated or error.
func (am *AbstractManager) PersistSingle(dataPointer interface{}, table string, provider DmlProvider) (inserted int, updated int, err error) {
	slice := convertToTypesSlice(dataPointer)
	inserted, updated, err = am.Manager.PersistAll(slice, table, provider)
	if err != nil {
		return 0, 0, err
	}
	if inserted > 0 {
		descriptor := am.RegisterDescriptorIfNeeded(table, dataPointer)
		if descriptor.Autoincrement {
			value := toolbox.GetSliceValue(slice, 0)
			reflect.ValueOf(dataPointer).Elem().Set(reflect.ValueOf(value))
		}
	}
	return inserted, updated, err
}

//PersistSingleOnConnection persists on connection single table row, dmlProvider is used to generate insert or udpate statement. It returns number of inserted, updated or error.
func (am *AbstractManager) PersistSingleOnConnection(connection Connection, dataPointer interface{}, table string, provider DmlProvider) (inserted int, updated int, err error) {
	slice := []interface{}{dataPointer}
	return am.Manager.PersistAllOnConnection(connection, &slice, table, provider)
}

//PersistData persist data on connection on table, keySetter is used to optionally set autoincrement column, sqlProvider handler will generate ParametrizedSQL with Insert or Update statement.
func (am *AbstractManager) PersistData(connection Connection, data []interface{}, table string, keySetter KeySetter, sqlProvider func(item interface{}) *ParametrizedSQL) (int, error) {
	var processed = 0
	for i, item := range data {
		parametrizedSQL := sqlProvider(item)
		result, err := am.Manager.ExecuteOnConnection(connection, parametrizedSQL.SQL, parametrizedSQL.Values)
		if err != nil {
			return 0, err
		}

		affected, err := result.RowsAffected()
		if err != nil {
			return 0, err
		}
		processed += int(affected)
		seq, lastInsertErr := result.LastInsertId()

		if lastInsertErr == nil && seq > 0 {
			structPointerValue := reflect.New(reflect.TypeOf(data[i]))
			reflect.Indirect(structPointerValue).Set(reflect.ValueOf(item))
			keySetter.SetKey(structPointerValue.Interface(), seq)
			data[i] = structPointerValue.Elem().Interface()

		}
	}
	return processed, nil
}

func (am *AbstractManager) fetchDataInBatches(connection Connection, sqlsWihtArguments []ParametrizedSQL, mapper RecordMapper) (*[][]interface{}, error) {
	var rows = make([][]interface{}, 0)
	for _, sqlWihtArguments := range sqlsWihtArguments {
		err := am.Manager.ReadAllOnConnection(connection, &rows, sqlWihtArguments.SQL, sqlWihtArguments.Values, mapper)
		if err != nil {
			return nil, err
		}
	}
	return &rows, nil
}

func (am *AbstractManager) fetchExistigData(connection Connection, table string, pkValues [][]interface{}, provider DmlProvider) ([][]interface{}, error) {
	var rows = make([][]interface{}, 0)
	descriptor := am.tableDescriptorRegistry.Get(table)
	if len(pkValues) > 0 {
		descriptor := TableDescriptor{Table: table, PkColumns: descriptor.PkColumns}
		sqlBuilder := NewQueryBuilder(&descriptor, "")
		sqlWithArguments := sqlBuilder.BuildBatchedQueryOnPk(descriptor.PkColumns, pkValues, batchSize)
		var mapper = NewColumnarRecordMapper(false, reflect.TypeOf(rows))
		batched, err := am.fetchDataInBatches(connection, sqlWithArguments, mapper)
		if err != nil {
			return nil, err
		}
		rows = append(rows, (*batched)...)
	}
	return rows, nil
}

//ClassifyDataAsInsertableOrUpdatable classifies passed in data as insertable or updatable.
func (am *AbstractManager) ClassifyDataAsInsertableOrUpdatable(connection Connection, dataPointer interface{}, table string, provider DmlProvider) ([]interface{}, []interface{}, error) {
	if provider == nil {
		return nil, nil, errors.New("Provider was nil")
	}

	var rowsByKey = make(map[string]interface{}, 0)
	var candidates, insertables, updatables = make([]interface{}, 0), make([]interface{}, 0), make([]interface{}, 0)
	var pkValues = make([][]interface{}, 0)

	toolbox.ProcessSlice(dataPointer, func(row interface{}) bool {
		var pkValueForThisRow = provider.Key(row)
		candidates = append(candidates, row)
		key := toolbox.JoinAsString(pkValueForThisRow, "")
		pkValues = append(pkValues, pkValueForThisRow)
		rowsByKey[key] = row
		return true
	})

	//fetch all existing pk values into rows to classify as updatable
	rows, err := am.fetchExistigData(connection, table, pkValues, provider)
	if err != nil {
		return nil, nil, fmt.Errorf("Failed to fetch existing data: due to:\n\t%v", err.Error())
	}

	//process existing rows and add mapped entires as updatables
	for _, row := range rows {
		key := toolbox.JoinAsString(row, "")
		if instance, ok := rowsByKey[key]; ok {
			updatables = append(updatables, instance)
			delete(rowsByKey, key)
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
func (am *AbstractManager) DeleteAll(dataPointer interface{}, table string, keyProvider KeyGetter) (deleted int, err error) {
	connection, err := am.Manager.ConnectionProvider().Get()
	if err != nil {
		return 0, err
	}
	err = connection.Begin()
	if err != nil {
		return 0, fmt.Errorf("Failed to start transaction on %v due to %v", am.config.Descriptor, err)
	}
	deleted, err = am.DeleteAllOnConnection(connection, dataPointer, table, keyProvider)
	if err == nil {
		commitErr := connection.Commit()
		if commitErr != nil {
			return 0, fmt.Errorf("Failed to commit on %v due to %v", am.config.Descriptor, commitErr)
		}
	} else {
		rollbackErr := connection.Rollback()
		if rollbackErr != nil {
			return 0, fmt.Errorf("Failed to rollback on %v due to %v, %v", am.config.Descriptor, err, rollbackErr)
		}
	}
	return deleted, err
}

//DeleteAllOnConnection deletes all rows on connection from table, key provider is used to extract primary keys. It returns number of deleted rows or error.
//If driver allows this operation is executed in one transaction.
func (am *AbstractManager) DeleteAllOnConnection(connection Connection, dataPointer interface{}, table string, keyProvider KeyGetter) (deleted int, err error) {

	deleted = 0
	structType := toolbox.DiscoverTypeByKind(dataPointer, reflect.Struct)
	keyProvider = NewKeyGetterIfNeeded(keyProvider, table, structType)
	am.RegisterDescriptorIfNeeded(table, dataPointer)

	descriptor := am.tableDescriptorRegistry.Get(table)
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
		result, err = am.Manager.ExecuteOnConnection(connection, dml, keyProvider.Key(item))
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
func (am *AbstractManager) DeleteSingle(dataPointer interface{}, table string, keyProvider KeyGetter) (bool, error) {
	connection, err := am.Manager.ConnectionProvider().Get()
	if err != nil {
		return false, err
	}
	err = connection.Begin()
	if err != nil {
		return false, fmt.Errorf("Failed to start transaction on %v due to %v", am.config.Descriptor, err)
	}
	suceess, err := am.DeleteSingleOnConnection(connection, dataPointer, table, keyProvider)
	if err == nil {
		commitErr := connection.Commit()
		if commitErr != nil {
			return false, fmt.Errorf("Failed to commit on %v due to %v", am.config.Descriptor, commitErr)
		}
	} else {
		rollbackErr := connection.Rollback()
		if rollbackErr != nil {
			return false, fmt.Errorf("Failed to rollback on %v due to %v, %v", am.config.Descriptor, err, rollbackErr)
		}
	}
	return suceess, err
}

func convertToTypesSlice(dataPointer interface{}) interface{} {
	toolbox.AssertPointerKind(dataPointer, reflect.Struct, "slicePointer")
	sliceValue := reflect.ValueOf(dataPointer).Elem()
	sliceType := reflect.SliceOf(sliceValue.Type())
	slicePointer := reflect.New(sliceType)
	slice := slicePointer.Elem()
	slice.Set(reflect.Append(slice, sliceValue))
	return slicePointer.Interface()
}

//DeleteSingleOnConnection deletes data on connection from table on for passed in data pointer, key provider is used to extract primary keys. It returns true if successful.
func (am *AbstractManager) DeleteSingleOnConnection(connection Connection, dataPointer interface{}, table string, keyProvider KeyGetter) (bool, error) {
	toolbox.AssertPointerKind(dataPointer, reflect.Struct, "dataPointer")
	slice := convertToTypesSlice(dataPointer)
	deleted, err := am.Manager.DeleteAllOnConnection(connection, slice, table, keyProvider)
	if err != nil {
		return false, err
	}
	return deleted == 1, nil
}

//ExpandSQL expands sql with passed in arguments
func (am *AbstractManager) ExpandSQL(sql string, arguments []interface{}) string {
	for _, arg := range arguments {
		var stringArg = toolbox.AsString(arg)
		if toolbox.IsString(arg) || toolbox.CanConvertToString(arg) {
			stringArg = "'" + stringArg + "'"
		}
		sql = strings.Replace(sql, "?", stringArg, 1)
	}
	return sql
}

//TableDescriptorRegistry returns a table descriptor registry
func (am *AbstractManager) TableDescriptorRegistry() TableDescriptorRegistry {
	return am.tableDescriptorRegistry
}

//NewAbstractManager create a new abstract manager, it takes config, conneciton provider, and target (sub class) manager
func NewAbstractManager(config *Config, connectionProvider ConnectionProvider, self Manager) *AbstractManager {
	return &AbstractManager{config: config, connectionProvider: connectionProvider, Manager: self, tableDescriptorRegistry: NewTableDescriptorRegistry()}
}
