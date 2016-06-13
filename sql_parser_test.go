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
package dsc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/viant/dsc"
)

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
			assert.Equal(t, "AND", query.Criteria[0].LogicalOperator)

		}
		{
			assert.Equal(t, "column2", query.Criteria[1].LeftOperand)
			assert.Equal(t, "!=", query.Criteria[1].Operator)
			assert.Equal(t, "?", query.Criteria[1].RightOperand)
			assert.Equal(t, "", query.Criteria[1].LogicalOperator)

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
		query, err := parser.Parse("SELECT key, username, active, salary, comments,last_access_time FROM users WHERE key = ?")
		if err != nil {
			t.Fatalf(err.Error())
		}
		assert.NotNil(t, query, "should have query")
	}

	//SELECT id, username, active, salary, comments,last_access_time FROM users WHERE key = ?

}

func TestInsertStatement(t *testing.T) {
	parse := dsc.NewDmlParser()
	statement, err := parse.Parse("INSERT INTO users(id, name, last_access_time) VALUES(?, ?, 2 )")
	assert.Nil(t, err, "should not have errors")
	assert.Equal(t, "INSERT", statement.Type)
	assert.Equal(t, "users", statement.Table)
	assert.Equal(t, 3, len(statement.Columns))
	assert.Equal(t, 3, len(statement.Values))
	assert.Equal(t, "2", statement.Values[2])
}

func TestUpdateStatement(t *testing.T) {
	parse := dsc.NewDmlParser()
	statement, err := parse.Parse("UPDATE users SET name = 'Smith', last_access_time = ? WHERE id = 2")
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

func TestDeleteStatement(t *testing.T) {
	parse := dsc.NewDmlParser()
	statement, err := parse.Parse("DELETE FROM users WHERE id = 2")
	assert.Nil(t, err, "should not have errors")
	assert.Equal(t, "DELETE", statement.Type)
	assert.Equal(t, "users", statement.Table)
	assert.Equal(t, 1, len(statement.Criteria))
	assert.Equal(t, "id", statement.Criteria[0].LeftOperand)
	assert.Equal(t, "=", statement.Criteria[0].Operator)
	assert.Equal(t, "2", statement.Criteria[0].RightOperand)

}
