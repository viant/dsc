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
	table := qb.TableDescriptor.From()
	return &ParametrizedSQL{
		SQL:    fmt.Sprintf(queryAllSQLTemplate, columnsLiteral, table),
		Values: make([]interface{}, 0),
	}

}

//BuildQueryOnPk builds ParametrizedSQL for passed in query columns and pk values.
func (qb *QueryBuilder) BuildQueryOnPk(columns []string, pkRowValues [][]interface{}) *ParametrizedSQL {
	return qb.BuildQueryWithInColumns(columns, append([]string{}, qb.TableDescriptor.PkColumns...), pkRowValues)
}

//BuildQueryOnPk builds ParametrizedSQL for passed in query columns and pk values.
func (qb *QueryBuilder) BuildQueryWithInColumns(columns []string, inCriteriaColumns []string, pkRowValues [][]interface{}) *ParametrizedSQL {
	columns = append([]string{}, columns...)
	updateReserved(columns)
	var columnsLiteral = qb.QueryHint + " " + strings.Join(columns, ",")
	updateReserved(inCriteriaColumns)
	var inColumns = strings.Join(inCriteriaColumns, ",")
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

	var whereCriteria = inColumns + " IN (" + criteria + ")"
	if multiValuePk {
		whereCriteria = "(" + inColumns + ") IN (" + criteria + ")"
	}
	table := qb.TableDescriptor.From()
	return &ParametrizedSQL{
		SQL:    fmt.Sprintf(querySQLTemplate, columnsLiteral, table, whereCriteria),
		Values: sqlArguments,
	}

}

//BuildBatchedQueryOnPk builds batches of ParametrizedSQL for passed in query columns and pk values. Batch size specifies number of rows in one parametrized sql.
func (qb *QueryBuilder) BuildBatchedInQuery(columns []string, pkRowValues [][]interface{}, inColumns []string, batchSize int) []*ParametrizedSQL {
	var result = make([]*ParametrizedSQL, 0)
	toolbox.Process2DSliceInBatches(pkRowValues, batchSize, func(batch [][]interface{}) {
		sqlWithArguments := qb.BuildQueryWithInColumns(columns, inColumns, batch)
		result = append(result, sqlWithArguments)
	})
	return result
}

//BuildBatchedQueryOnPk builds batches of ParametrizedSQL for passed in query columns and pk values. Batch size specifies number of rows in one parametrized sql.
func (qb *QueryBuilder) BuildBatchedQueryOnPk(columns []string, pkRowValues [][]interface{}, batchSize int) []*ParametrizedSQL {
	var result = make([]*ParametrizedSQL, 0)
	toolbox.Process2DSliceInBatches(pkRowValues, batchSize, func(batch [][]interface{}) {
		sqlWithArguments := qb.BuildQueryOnPk(columns, batch)
		result = append(result, sqlWithArguments)
	})
	return result
}

//NewQueryBuilder create anew QueryBuilder, it takes table descriptor and optional query hit to include it in the queries
func NewQueryBuilder(descriptor *TableDescriptor, queryHint string) QueryBuilder {
	queryBuilder := QueryBuilder{TableDescriptor: descriptor, QueryHint: queryHint}
	return queryBuilder
}
