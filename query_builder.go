package dsc

import (
	"fmt"
	"strings"

	"github.com/viant/toolbox"
)

var queryAllSQLTemplate = "SELECT %v FROM %v"

//QueryBuilder represetns a query builder. It builds simple select sql.
type QueryBuilder struct {
	QueryHint       string
	TableDescriptor *TableDescriptor
}

//BuildQueryAll builds query all data without where clause
func (qb *QueryBuilder) BuildQueryAll(columns []string) *ParametrizedSQL {
	var columnsLiteral = qb.QueryHint + " " + strings.Join(columns, ",")
	table := qb.TableDescriptor.Table
	return &ParametrizedSQL{
		SQL:    fmt.Sprintf(queryAllSQLTemplate, columnsLiteral, table),
		Values: make([]interface{}, 0),
	}

}

//BuildQueryOnPk builds ParametrizedSQL for passed in query columns and pk values.
func (qb *QueryBuilder) BuildQueryOnPk(columns []string, pkRowValues [][]interface{}) *ParametrizedSQL {
	var columnsLiteral = qb.QueryHint + " " + strings.Join(columns, ",")
	var pkColumns = strings.Join(qb.TableDescriptor.PkColumns, ",")
	var sqlArguments = make([]interface{}, 0)
	var criteria = ""
	var multiValuePk = false
	for _, pkValues := range pkRowValues {
		if len(pkValues) > 1 {
			multiValuePk = true
		}
		var rowCriteria = strings.Repeat("?,", len(pkValues))
		rowCriteria = rowCriteria[0 : len(rowCriteria)-1]

		sqlArguments = append(sqlArguments, pkValues...)
		if len(criteria) > 0 {
			criteria = criteria + ","
		}
		if multiValuePk {
			criteria = criteria + "(" + rowCriteria + ")"
		} else {
			criteria = criteria + rowCriteria
		}
	}

	var whereCriteria = pkColumns + " IN (" + criteria + ")"
	if multiValuePk {
		whereCriteria = "(" + pkColumns + ") IN (" + criteria + ")"
	}
	table := qb.TableDescriptor.Table
	return &ParametrizedSQL{
		SQL:    fmt.Sprintf(querySQLTemplate, columnsLiteral, table, whereCriteria),
		Values: sqlArguments,
	}

}

//BuildBatchedQueryOnPk builds batches of ParametrizedSQL for passed in query columns and pk values. Batch size specifies number of rows in one parametrized sql.
func (qb *QueryBuilder) BuildBatchedQueryOnPk(columns []string, pkRowValues [][]interface{}, batchSize int) []ParametrizedSQL {
	var result = make([]ParametrizedSQL, 0)
	toolbox.Process2DSliceInBatches(pkRowValues, batchSize, func(batch [][]interface{}) {
		sqlWithArguments := qb.BuildQueryOnPk(columns, batch)
		result = append(result, *sqlWithArguments)
	})
	return result
}

//NewQueryBuilder create anew QueryBuilder, it takes table descriptor and optional query hit to include it in the queries
func NewQueryBuilder(descriptor *TableDescriptor, queryHint string) QueryBuilder {
	queryBuilder := QueryBuilder{TableDescriptor: descriptor, QueryHint: queryHint}
	return queryBuilder
}
