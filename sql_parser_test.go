package dsc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
	"github.com/viant/toolbox"
)

func TestAggregationQueryParser(t *testing.T) {

	parser := dsc.NewQueryParser()
	{
		query, err := parser.Parse("SELECT col1, SUM(col2) FROM bar GROUP BY 1")
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, query, "should have query")
		assert.Equal(t, 2, len(query.Columns))

		assert.Equal(t, "col1", query.Columns[0].Name)

		assert.Equal(t, "SUM(col2)", query.Columns[1].Expression)

		assert.Equal(t, "SUM", query.Columns[1].Function)
		assert.Equal(t, "col2", query.Columns[1].FunctionArguments)

		assert.Equal(t, "f1", query.Columns[1].Alias)

		assert.Equal(t, 1, len(query.GroupBy))
		assert.Equal(t, "col1", query.Columns[0].Name)

		assert.Equal(t, "bar", query.Table)
	}

	{
		query, err := parser.Parse("SELECT col1, SUM(col2) FROM bar WHERE col3 > 7 GROUP BY 1")
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, query, "should have query")
		assert.Equal(t, 2, len(query.Columns))

		assert.Equal(t, "col1", query.Columns[0].Name)
		assert.Equal(t, "SUM(col2)", query.Columns[1].Expression)
		assert.Equal(t, "SUM", query.Columns[1].Function)
		assert.Equal(t, "col2", query.Columns[1].FunctionArguments)
		assert.Equal(t, "f1", query.Columns[1].Alias)
		assert.Equal(t, 1, len(query.GroupBy))
		assert.Equal(t, "col1", query.Columns[0].Name)
		assert.Equal(t, "bar", query.Table)
	}

}

func TestQueryParser(t *testing.T) {
	parser := dsc.NewQueryParser()
	{
		query, err := parser.Parse("SELECT abc FROM bar")
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, query, "should have query")
		assert.Equal(t, 1, len(query.Columns))
		assert.Equal(t, "abc", query.Columns[0].Name)
		assert.Equal(t, "bar", query.Table)

	}

	{
		query, err := parser.Parse("SELECT  id,\nevent_type,\nquantity,\ntimestamp,\nquery_string\nFROM events t")
		assert.Nil(t, err)
		assert.Equal(t, 5, len(query.Columns))
		assert.Equal(t, "id", query.Columns[0].Name)
		assert.Equal(t, "events", query.Table)
		assert.Equal(t, "t", query.Alias)
	}

	{
		query, err := parser.Parse("SELECT c1, c2 FROM bar")
		assert.Nil(t, err)
		assert.Equal(t, []string{"c1", "c2"}, query.ColumnNames())
	}

	{
		query, err := parser.Parse("SELECT * FROM foo")
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, query, "should have query")
		assert.Equal(t, true, query.AllField)
		assert.Equal(t, "foo", query.Table)

	}

	{
		query, err := parser.Parse("SELECT * FROM foo WHERE column1 = 2")
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, query, "should have query")
		assert.Equal(t, true, query.AllField)
		assert.Equal(t, "foo", query.Table)
		assert.Equal(t, 1, len(query.Criteria))
		assert.Equal(t, "column1", query.Criteria[0].LeftOperand)
		assert.Equal(t, "=", query.Criteria[0].Operator)
		assert.Equal(t, "2", query.Criteria[0].RightOperand)

	}

	{
		query, err := parser.Parse("SELECT * FROM foo WHERE column1 = 2 AND column2 != ?")
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, query, "should have query")
		assert.Equal(t, true, query.AllField)
		assert.Equal(t, "foo", query.Table)
		assert.Equal(t, 2, len(query.Criteria))
		{
			assert.Equal(t, "column1", query.Criteria[0].LeftOperand)
			assert.Equal(t, "=", query.Criteria[0].Operator)
			assert.Equal(t, "2", query.Criteria[0].RightOperand)
			assert.Equal(t, "AND", query.LogicalOperator)

		}
		{
			assert.Equal(t, "column2", query.Criteria[1].LeftOperand)
			assert.Equal(t, "!=", query.Criteria[1].Operator)
			assert.Equal(t, "?", query.Criteria[1].RightOperand)
			assert.Equal(t, "AND", query.LogicalOperator)

		}

	}

	{
		query, err := parser.Parse("SELECT abc FROM bar WHERE id IN (1, 2, ?)")
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, query, "should have query")
		assert.Equal(t, 1, len(query.Columns))
		assert.Equal(t, "abc", query.Columns[0].Name)
		assert.Equal(t, "bar", query.Table)
		assert.Equal(t, 1, len(query.Criteria))
		assert.Equal(t, 3, len(query.Criteria[0].RightOperands))

		assert.EqualValues(t, "1", query.Criteria[0].RightOperands[0])
		assert.Equal(t, "?", query.Criteria[0].RightOperands[2])
	}

	{
		query, err := parser.Parse("SELECT abc FROM bar WHERE id NOT IN (1, 2, ?)")
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, query, "should have query")
		assert.Equal(t, 1, len(query.Columns))
		assert.Equal(t, "abc", query.Columns[0].Name)
		assert.Equal(t, "bar", query.Table)
		assert.Equal(t, 1, len(query.Criteria))
		assert.True(t, query.Criteria[0].Inverse)
		assert.Equal(t, 3, len(query.Criteria[0].RightOperands))

		assert.EqualValues(t, "1", query.Criteria[0].RightOperands[0])
		assert.Equal(t, "?", query.Criteria[0].RightOperands[2])
	}

	{
		query, err := parser.Parse("SELECT key, username, active, salary, comments,last_access_time FROM users WHERE key = ?")
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, query, "should have query")
	}

	//SELECT id, username, active, salary, comments,last_access_time FROM users WHERE key = ?

	{
		query, err := parser.Parse("SELECT * FROM foo WHERE id IS NULL")
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, query, "should have query")
		assert.Equal(t, true, query.AllField)
		assert.Equal(t, "foo", query.Table)
		assert.Equal(t, "id", query.Criteria[0].LeftOperand)
		assert.Equal(t, "IS", query.Criteria[0].Operator)
		assert.Nil(t, query.Criteria[0].RightOperand)

	}

	{
		query, err := parser.Parse("SELECT * FROM foo WHERE id BETWEEN 1 AND 2")
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, query, "should have query")
		assert.Equal(t, true, query.AllField)
		assert.Equal(t, "foo", query.Table)
		assert.Equal(t, "id", query.Criteria[0].LeftOperand)
		assert.Equal(t, "BETWEEN", query.Criteria[0].Operator)
		assert.Equal(t, 2, len(query.Criteria[0].RightOperands))
		assert.Equal(t, "1", query.Criteria[0].RightOperands[0])
		assert.Equal(t, "2", query.Criteria[0].RightOperands[1])

	}

	{
		query, err := parser.Parse("SELECT * FROM foo WHERE id LIKE '%1%'")
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, query, "should have query")
		assert.Equal(t, true, query.AllField)
		assert.Equal(t, "foo", query.Table)
		assert.Equal(t, "id", query.Criteria[0].LeftOperand)
		assert.Equal(t, "LIKE", query.Criteria[0].Operator)
		assert.Equal(t, "'%1%'", query.Criteria[0].RightOperand)
	}

	{
		query, err := parser.Parse("SELECT foo, bar FROM table WHERE (foo, bar) IN ((1,2),(3,4)) ")
		assert.Nil(t, err)
		assert.Equal(t, false, query.AllField)
		assert.Equal(t, "table", query.Table)
		assert.Equal(t, "(foo, bar)", query.Criteria[0].LeftOperand)
		assert.Equal(t, "IN", query.Criteria[0].Operator)
		assert.Equal(t, []interface{}{
			"(1,2)",
			"(3,4)",
		}, query.Criteria[0].RightOperands)

	}

	{
		_, err := parser.Parse("SELECT* FROM foo")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT * FROM foo WHERE id IS a")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT * FROM foo WHERE id IS NOT a")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT * FROM foo WHERE id BETWEEN a b")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT * FROM foo WHERE id IN a")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT * FROM foo WHERE id NOT a")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT a v s FROM foo WHERE id NOT a")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse(",SELECT a v s FROM foo WHERE id NOT a")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse(",SELECT a ,FROM foo WHERE id NOT a")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse(",SELECT a FROM foo ,WHERE id NOT a")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT * a FROM foo ,WHERE id NOT a")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT a AS 1 FROM foo WHERE id NOT a")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT a FROM foo WHERE id BETWEEN")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT a FROM foo WHERE id BETWEEN 1")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT a FROM foo WHERE id BETWEEN 1 AND")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT a FROM foo WHERE id LIKE")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT a FROM foo WHERE id = 1 AVC")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT a FROM foo WHERE")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT a FROM foo WHERE ,")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT a FROM foo WHERE in IN(1,)")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT a FROM foo WHERE in IN(1")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("SELECT a")
		assert.NotNil(t, err)
	}

}

func TestInvalidDml(t *testing.T) {

	parser := dsc.NewDmlParser()
	{
		_, err := parser.Parse(".INSERT INTO users(id, name, last_access_time) VALUES(?, ?, 2 )")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse(".INSERT users(id, name, last_access_time) VALUES(?, ?, 2 )")
		assert.NotNil(t, err)
	}
	{
		_, err := parser.Parse("INSERT INTO ")
		assert.NotNil(t, err)
	}
	{
		_, err := parser.Parse("INSERT INTO users ")
		assert.NotNil(t, err)
	}
	{
		_, err := parser.Parse(".INSERT users(id,) VALUES(?, ?, 2 )")
		assert.NotNil(t, err)
	}
	{
		_, err := parser.Parse(".INSERT users(id VALUES(?, ?, 2 )")
		assert.NotNil(t, err)
	}
	{
		_, err := parser.Parse(".INSERT users(id)")
		assert.NotNil(t, err)
	}
	{
		_, err := parser.Parse("INSERT INTO users(id, name, last_access_time) VALUES")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("INSERT INTO ,")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("INSERT ,")
		assert.NotNil(t, err)
	}
	{
		_, err := parser.Parse("INSERT INTO users(id, name, last_access_time) VALUES(1,")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("INSERT INTO users(id,1) VALUES(1,")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("INSERT INTO users(id -) VALUES(1,")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("UPDATEusers SET name = 'Smith', last_access_time = ? WHERE id = 2")
		assert.NotNil(t, err)
	}
	{
		_, err := parser.Parse("UPDATE users")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("UPDATE users SE")
		assert.NotNil(t, err.Error())
	}

	{
		_, err := parser.Parse("UPDATE users SET")
		assert.NotNil(t, err)
	}
	{
		_, err := parser.Parse("UPDATEusers SET name WHERE id = 2")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("UPDATE users SET name = 1 WHERE")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("UPDATE users SET name = 1 WHERE ,")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("UPDATE users SET , = 1")
		assert.NotNil(t, err)
	}
	{
		_, err := parser.Parse("UPDATE users SET a ,")
		assert.NotNil(t, err)
	}
	{
		_, err := parser.Parse("UPDATE users SET a = ,")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("UPDATE users SET name =")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("DELETE")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("DELETE ,FROM")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("DELETE FROM ,")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("DELETE FROM users WHERE")
		assert.NotNil(t, err)
	}

	{
		_, err := parser.Parse("DELETE FROM users ,")
		assert.NotNil(t, err)
	}

}

func TestInsertStatement(t *testing.T) {
	parser := dsc.NewDmlParser()
	statement, err := parser.Parse("INSERT INTO users(id, name, last_access_time) VALUES(?, ?, 2 )")
	assert.Nil(t, err, "should not have errors")
	assert.Equal(t, "INSERT", statement.Type)
	assert.Equal(t, "users", statement.Table)
	assert.Equal(t, 3, len(statement.Columns))
	assert.Equal(t, 3, len(statement.Values))
	assert.Equal(t, "2", statement.Values[2])
}

func TestUpdateStatement(t *testing.T) {
	parser := dsc.NewDmlParser()

	{
		statement, err := parser.Parse("UPDATE users SET name = 'Smith', last_access_time = ?")
		assert.Nil(t, err, "should not have errors")
		assert.Equal(t, "UPDATE", statement.Type)
		assert.Equal(t, "users", statement.Table)
		assert.Equal(t, 2, len(statement.Columns))
		assert.Equal(t, 2, len(statement.Values))
		assert.Equal(t, "'Smith'", statement.Values[0])
		assert.Equal(t, 0, len(statement.Criteria))
	}
	{
		statement, err := parser.Parse("UPDATE users SET name = 'Smith', last_access_time = ? WHERE id = 2")
		assert.Nil(t, err, "should not have errors")
		assert.Equal(t, "UPDATE", statement.Type)
		assert.Equal(t, "users", statement.Table)
		assert.Equal(t, 2, len(statement.Columns))
		assert.Equal(t, 2, len(statement.Values))
		assert.Equal(t, "'Smith'", statement.Values[0])
		assert.Equal(t, 1, len(statement.Criteria))
		assert.Equal(t, "id", statement.Criteria[0].LeftOperand)
		assert.Equal(t, "=", statement.Criteria[0].Operator)
		assert.Equal(t, "2", statement.Criteria[0].RightOperand)
	}

}

func TestDeleteStatement(t *testing.T) {
	parse := dsc.NewDmlParser()
	{
		statement, err := parse.Parse("DELETE FROM users ")
		assert.Nil(t, err, "should not have errors")
		assert.Equal(t, "DELETE", statement.Type)
		assert.Equal(t, "users", statement.Table)
		assert.Equal(t, 0, len(statement.Criteria))

	}

	{
		statement, err := parse.Parse("DELETE FROM users WHERE id = 2")
		assert.Nil(t, err, "should not have errors")
		assert.Equal(t, "DELETE", statement.Type)
		assert.Equal(t, "users", statement.Table)
		assert.Equal(t, 1, len(statement.Criteria))
		assert.Equal(t, "id", statement.Criteria[0].LeftOperand)
		assert.Equal(t, "=", statement.Criteria[0].Operator)
		assert.Equal(t, "2", statement.Criteria[0].RightOperand)

	}

}

func TestCriteriaValues(t *testing.T) {
	parser := dsc.NewQueryParser()

	query, err := parser.Parse("SELECT * FROM foo WHERE column1 = 2 AND column2 = ? AND column3 = 'abc' AND column4 = ? AND column5 = true")
	assert.Nil(t, err)
	{
		iterator := toolbox.NewSliceIterator([]string{"3", "a"})
		values, err := query.CriteriaValues(iterator)
		assert.Nil(t, err)
		assert.Equal(t, 5, len(values))
		assert.Equal(t, []interface{}{"2", "3", "abc", "a", "true"}, values)
	}
	{
		iterator := toolbox.NewSliceIterator([]string{"3"})
		_, err = query.CriteriaValues(iterator)
		assert.NotNil(t, err)
	}
	{
		parse := dsc.NewDmlParser()
		statement, err := parse.Parse("UPDATE users SET a = ? WHERE id = ?")
		iterator := toolbox.NewSliceIterator([]string{})
		_, err = statement.ColumnValueMap(iterator)
		assert.NotNil(t, err)
	}

	{
		parse := dsc.NewDmlParser()
		statement, err := parse.Parse("UPDATE users SET a = ? WHERE id = ?")
		iterator := toolbox.NewSliceIterator([]string{})
		_, err = statement.ColumnValues(iterator)
		assert.NotNil(t, err)
	}
}
