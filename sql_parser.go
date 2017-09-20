package dsc

import (
	"errors"
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)

//SQLColumn represents a sql column
type SQLColumn struct {
	Name              string
	Alias             string
	Expression        string
	Function          string
	FunctionArguments string
}

//SQLCriterion represents single where clause condiction
type SQLCriterion struct {
	LeftOperand     interface{}
	Operator        string
	RightOperand    interface{}
	RightOperands   []interface{}
	Inverse         bool // if not operator presents
	LogicalOperator string
}

//BaseStatement represents a base query and dml statement
type BaseStatement struct {
	SQL      string
	Table    string
	Columns  []*SQLColumn
	Criteria []*SQLCriterion
}

//ColumnNames returns a column names.
func (bs BaseStatement) ColumnNames() []string {
	var result = make([]string, 0)
	for _, column := range bs.Columns {
		result = append(result, column.Name)
	}
	return result
}

func bindValueIfNeeded(source interface{}, parameters toolbox.Iterator) (interface{}, error) {
	textOperand := toolbox.AsString(source)
	if textOperand == "?" {
		if !parameters.HasNext() {
			return nil, errors.New("Unable to bind value - not enough parameters")
		}
		var values = make([]interface{}, 1)
		err := parameters.Next(&values[0])
		if err != nil {
			return nil, err
		}
		return values[0], nil
	}
	if strings.HasPrefix(textOperand, "'") {
		return textOperand[1 : len(textOperand)-1], nil
	}

	if !toolbox.CanConvertToString(source) {
		value, _ := toolbox.DiscoverValueAndKind(textOperand)
		return value, nil
	}
	return source, nil
}

//CriteriaValues returns criteria values  extracted from binding parameters, starting from parametersOffset,
func (bs BaseStatement) CriteriaValues(parameters toolbox.Iterator) ([]interface{}, error) {
	var values = make([]interface{}, 0)
	for _, criterion := range bs.Criteria {
		var criterionValues = criterion.RightOperands
		if len(criterionValues) == 0 {
			criterionValues = []interface{}{criterion.RightOperand}
		}
		for i := range criterionValues {
			value, err := bindValueIfNeeded(criterionValues[i], parameters)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
	}
	return values, nil
}

//QueryStatement represents SQL query statement.
type QueryStatement struct {
	BaseStatement
	AllField    bool
	UnionTables []string
	GroupBy     []*SQLColumn
}

//DmlStatement represents dml statement.
type DmlStatement struct {
	BaseStatement
	Type   string
	Values []interface{}
}

//ColumnValues returns values of columns extracted from binding parameters
func (ds DmlStatement) ColumnValues(parameters toolbox.Iterator) ([]interface{}, error) {
	var values = make([]interface{}, 0)
	for i := range ds.Columns {
		value, err := bindValueIfNeeded(ds.Values[i], parameters)
		if err != nil {
			return nil, err
		}
		values = append(values, value)

	}
	return values, nil
}

//ColumnValueMap returns map of column with its values extracted from passed in parameters
func (ds DmlStatement) ColumnValueMap(parameters toolbox.Iterator) (map[string]interface{}, error) {
	var result = make(map[string]interface{})
	columnValues, err := ds.ColumnValues(parameters)
	if err != nil {
		return nil, err
	}
	for i, column := range ds.Columns {
		result[column.Name] = columnValues[i]
	}
	return result, nil
}

const (
	undefined int = iota
	eof
	illegal
	whitespaces
	id
	asterisk
	coma
	operator
	equalOperator
	logicalOperator
	betweenOperatorKeyword
	betweenAndKeyword
	inOperatorKeyword
	nullKeyword
	likeOperatorKeyword
	isOperatorKeyword
	sqlValue
	sqlValues
	notKeyword
	groupingBegin
	groupingEnd
	expression
	selectKeyword
	fromKeyword
	whereKeyword
	asKeyword
	groupKeyword
	byKeyword
	columnRef
	insertKeyword
	intoKeyword
	valuesKeyword

	updateKeyword
	deleteKeyword
	setKeyword
)

var sqlMatchers = map[int]toolbox.Matcher{
	eof:         toolbox.EOFMatcher{},
	whitespaces: toolbox.CharactersMatcher{" \n\t"},
	id:          toolbox.LiteralMatcher{},
	asterisk:    toolbox.CharactersMatcher{"*"},
	coma:        toolbox.CharactersMatcher{","},
	operator: toolbox.KeywordsMatcher{
		Keywords:      []string{"=", ">=", "<=", "<>", ">", "<", "!="},
		CaseSensitive: false,
	},
	notKeyword: toolbox.KeywordsMatcher{
		Keywords:      []string{"NOT"},
		CaseSensitive: false,
	},
	likeOperatorKeyword: toolbox.KeywordsMatcher{
		Keywords:      []string{"LIKE"},
		CaseSensitive: false,
	},
	isOperatorKeyword: toolbox.KeywordsMatcher{
		Keywords:      []string{"IS"},
		CaseSensitive: false,
	},
	betweenOperatorKeyword: toolbox.KeywordsMatcher{
		Keywords:      []string{"BETWEEN"},
		CaseSensitive: false,
	},
	betweenAndKeyword: toolbox.KeywordsMatcher{
		Keywords:      []string{"AND"},
		CaseSensitive: false,
	},
	nullKeyword: toolbox.KeywordsMatcher{
		Keywords:      []string{"NULL"},
		CaseSensitive: false,
	},
	equalOperator:     toolbox.KeywordsMatcher{Keywords: []string{"="}, CaseSensitive: false},
	inOperatorKeyword: toolbox.KeywordMatcher{Keyword: "IN", CaseSensitive: false},
	logicalOperator: toolbox.KeywordsMatcher{
		Keywords:      []string{"AND", "OR"},
		CaseSensitive: false,
	},
	sqlValue: valueMatcher{optionallyEnclosingChar: "'", terminatorChars: ",\t\n) "},
	sqlValues: valuesMatcher{
		valuesGroupingBeginChar:         "(",
		valuesGroupingEndChar:           ")",
		valueSeparator:                  ",",
		valueOptionallyEnclosedWithChar: "'",
		valueTerminatorCharacters:       ", \n\t)",
	},
	columnRef:     &toolbox.IntMatcher{},
	groupingBegin: toolbox.CharactersMatcher{"("},
	groupingEnd:   toolbox.CharactersMatcher{")"},
	expression:    &toolbox.BodyMatcher{"(", ")"},
	groupKeyword:  toolbox.KeywordMatcher{Keyword: "GROUP", CaseSensitive: false},
	byKeyword:     toolbox.KeywordMatcher{Keyword: "BY", CaseSensitive: false},
	selectKeyword: toolbox.KeywordMatcher{Keyword: "SELECT", CaseSensitive: false},
	fromKeyword:   toolbox.KeywordMatcher{Keyword: "FROM", CaseSensitive: false},
	whereKeyword:  toolbox.KeywordMatcher{Keyword: "WHERE", CaseSensitive: false},
	asKeyword:     toolbox.KeywordMatcher{Keyword: "AS", CaseSensitive: false},
	insertKeyword: toolbox.KeywordMatcher{Keyword: "INSERT", CaseSensitive: false},
	intoKeyword:   toolbox.KeywordMatcher{Keyword: "INTO", CaseSensitive: false},
	valuesKeyword: toolbox.KeywordMatcher{Keyword: "VALUES", CaseSensitive: false},

	updateKeyword: toolbox.KeywordMatcher{Keyword: "UPDATE", CaseSensitive: false},
	setKeyword:    toolbox.KeywordMatcher{Keyword: "SET", CaseSensitive: false},

	deleteKeyword: toolbox.KeywordMatcher{Keyword: "DELETE", CaseSensitive: false},
}

type baseParser struct{}

func (bp *baseParser) expectWhitespaceFollowedBy(tokenizer *toolbox.Tokenizer, expectedTokensMessage string, expected ...int) (*toolbox.Token, error) {
	token := tokenizer.Next(whitespaces)

	if token.Token == eof && !toolbox.HasSliceAnyElements(expected, eof) {
		return nil, newIllegalTokenParsingError(tokenizer.Index, expectedTokensMessage)
	}
	if token.Token == illegal {
		return nil, newIllegalTokenParsingError(tokenizer.Index, "whitespace")
	}
	if token.Token == whitespaces {
		token = tokenizer.Nexts(expected...)
	}
	if token.Token == illegal {
		return nil, newIllegalTokenParsingError(tokenizer.Index, expectedTokensMessage)
	}
	if token.Token == eof && len(token.Matched) > 0 {
		return nil, newIllegalTokenParsingError(tokenizer.Index, expectedTokensMessage)
	}
	return token, nil
}

func (bp *baseParser) expectOptionalWhitespaceFollowedBy(tokenizer *toolbox.Tokenizer, expectedTokensMessage string, expected ...int) (*toolbox.Token, error) {
	var expectedTokens = make([]int, 0)
	expectedTokens = append(expectedTokens, whitespaces)
	expectedTokens = append(expectedTokens, expected...)

	token := tokenizer.Nexts(expectedTokens...)

	if token.Token == eof && !toolbox.HasSliceAnyElements(expectedTokens, eof) {
		return nil, newIllegalTokenParsingError(tokenizer.Index, expectedTokensMessage)
	}

	if token.Token == illegal {
		return nil, newIllegalTokenParsingError(tokenizer.Index, expectedTokensMessage)
	}
	if token.Token == whitespaces {
		token = tokenizer.Nexts(expected...)
	}
	if token.Token == illegal {
		return nil, newIllegalTokenParsingError(tokenizer.Index, expectedTokensMessage)
	}
	if token.Token == eof && len(token.Matched) > 0 {
		return nil, newIllegalTokenParsingError(tokenizer.Index, expectedTokensMessage)
	}
	return token, nil
}

func (bp *baseParser) readValues(values string) ([]interface{}, error) {
	var result = make([]interface{}, 0)
	tokenizer := toolbox.NewTokenizer(values, illegal, eof, sqlMatchers)
	for {
		token, err := bp.expectOptionalWhitespaceFollowedBy(tokenizer, "value", sqlValue)
		if err != nil {
			return nil, err
		}
		result = append(result, token.Matched)

		token, err = bp.expectOptionalWhitespaceFollowedBy(tokenizer, ",|eof", coma, eof)
		if err != nil {
			return nil, err
		}

		if token.Token == eof {
			break
		}
	}
	return result, nil
}

func (bp *baseParser) readInValues(tokenizer *toolbox.Tokenizer) (string, []interface{}, error) {
	token, err := bp.expectOptionalWhitespaceFollowedBy(tokenizer, "(value [,..])", sqlValues)
	if err != nil {
		return "", nil, err
	}
	value := token.Matched
	values, err := bp.readValues(value[1 : len(value)-1])
	if err != nil {
		return "", nil, err
	}
	return value, values, nil
}

func (bp *baseParser) readCriteria(tokenizer *toolbox.Tokenizer, statement *BaseStatement, token *toolbox.Token) (err error) {
	statement.Criteria = make([]*SQLCriterion, 0)
	for {
		token, err = bp.expectWhitespaceFollowedBy(tokenizer, "value", sqlValue)
		if err != nil {
			return err
		}
		if token.Matched == "" {
			return fmt.Errorf("Expected criteria at %v", tokenizer.Index)
		}

		index := len(statement.Criteria)
		statement.Criteria = append(statement.Criteria, &SQLCriterion{LeftOperand: token.Matched})

		token, err = bp.expectOptionalWhitespaceFollowedBy(tokenizer, "operator", inOperatorKeyword, likeOperatorKeyword, betweenOperatorKeyword, notKeyword, isOperatorKeyword, operator, eof)
		if err != nil {
			return err
		}
		statement.Criteria[index].Operator = token.Matched
		switch token.Token {
		case notKeyword:
			statement.Criteria[index].Inverse = true
			token, err = bp.expectOptionalWhitespaceFollowedBy(tokenizer, "operator", inOperatorKeyword, likeOperatorKeyword)
			if err != nil {
				return err
			}
			statement.Criteria[index].Operator = token.Matched
			fallthrough
		case inOperatorKeyword:
			var value, values, err = bp.readInValues(tokenizer)
			statement.Criteria[index].RightOperand = value
			statement.Criteria[index].RightOperands = values
			if err != nil {
				return err
			}
		case likeOperatorKeyword, operator:
			token, err = bp.expectOptionalWhitespaceFollowedBy(tokenizer, "value", sqlValue)
			if err != nil {
				return err
			}
			statement.Criteria[index].RightOperand = token.Matched
		case isOperatorKeyword:
			token, err = bp.expectOptionalWhitespaceFollowedBy(tokenizer, "operator", notKeyword, nullKeyword)
			if err != nil {
				return err
			}
			if token.Token == notKeyword {
				statement.Criteria[index].Inverse = true
				token, err = bp.expectOptionalWhitespaceFollowedBy(tokenizer, "operator", nullKeyword)
				if err != nil {
					return err
				}
				statement.Criteria[index].RightOperand = token.Matched
			}
		case betweenOperatorKeyword:
			token, err = bp.expectOptionalWhitespaceFollowedBy(tokenizer, "value", sqlValue)
			if err != nil {
				return err
			}
			fromValue := token.Matched
			token, err = bp.expectOptionalWhitespaceFollowedBy(tokenizer, "and", betweenAndKeyword)
			if err != nil {
				return err
			}
			token, err = bp.expectOptionalWhitespaceFollowedBy(tokenizer, "value", sqlValue)
			if err != nil {
				return err
			}
			toValue := token.Matched
			statement.Criteria[index].RightOperands = []interface{}{fromValue, toValue}
		}
		token, err = bp.expectOptionalWhitespaceFollowedBy(tokenizer, "or | and | eof", eof, logicalOperator, groupKeyword)
		if err != nil {
			return err
		}
		if token.Token == eof {
			break
		}
		if token.Token == groupKeyword {
			tokenizer.Index -= len(token.Matched)
			break
		}
		statement.Criteria[index].LogicalOperator = token.Matched
	}
	return nil
}

//QueryParser represents a simple SQL query parser.
type QueryParser struct{ baseParser }

func buildColumn(token *toolbox.Token, tokenizer *toolbox.Tokenizer, query *QueryStatement, isGroupBy bool) (*SQLColumn, error) {
	column := &SQLColumn{}
	if token.Token == expression {
		column.Expression = token.Matched
	} else if token.Token == columnRef {
		if !isGroupBy {
			return nil, fmt.Errorf("Invalid token at %v expected ',' 'FROM' or alias", tokenizer.Index)
		}
		columnPosition := toolbox.AsInt(token.Matched)
		if !(columnPosition-1 < len(query.Columns)) {
			return nil, fmt.Errorf("Invalid colum reference %v at %v", columnPosition, tokenizer.Index)
		}
		column = query.Columns[columnPosition-1]
	} else {
		column.Name = token.Matched
	}
	return column, nil
}

func (qp *QueryParser) readQueryColumns(tokenizer *toolbox.Tokenizer, query *QueryStatement, columns *[]*SQLColumn, token *toolbox.Token, isGroupBy bool, terminationTokens ...int) error {
	var err error
	*columns = make([]*SQLColumn, 0)
	column, err := buildColumn(token, tokenizer, query, isGroupBy)
	if err != nil {
		return err
	}
	*columns = append(*columns, column)
outer:
	for {
		token = tokenizer.Nexts(whitespaces, id, coma, eof, expression)
		switch token.Token {
		case illegal, eof:
			if isGroupBy {
				return nil
			}
			return fmt.Errorf("Invalid token at %v expected ',' 'FROM' or alias", tokenizer.Index)

		case expression:
			column.Expression = column.Name + " " + token.Matched
			column.Function = column.Name
			if len(token.Matched) > 2 {
				column.FunctionArguments = string(token.Matched[1 : len(token.Matched)-1])
			}
			column.Name = ""

		case coma:
			token, err = qp.expectOptionalWhitespaceFollowedBy(tokenizer, "column", id, expression)
			if err != nil {
				return err
			}
			column, err = buildColumn(token, tokenizer, query, isGroupBy)
			if err != nil {
				return err
			}
			*columns = append(*columns, column)
			break
		case whitespaces:

			var next = make([]int, 0)
			next = append(next, terminationTokens...)
			next = append(next, asKeyword, id)
			nextToken := tokenizer.Nexts(next...)
			for _, candidate := range terminationTokens {
				if nextToken.Token == candidate {
					break outer
				}
			}
			if nextToken.Token == asKeyword {
				token, err = qp.expectWhitespaceFollowedBy(tokenizer, "alias", id)
				if err != nil {
					return err
				}
				(*columns)[len(*columns)-1].Alias = token.Matched
			}

		}
	}

	var expressionAlias = 1
	for _, column := range *columns {
		if column.Expression != "" && column.Alias == "" {
			column.Alias = "f" + toolbox.AsString(expressionAlias)
			expressionAlias++
		}
	}
	return nil
}

//Parse parses SQL query to build QueryStatement
func (qp *QueryParser) Parse(query string) (*QueryStatement, error) {
	tokenizer := toolbox.NewTokenizer(query, illegal, eof, sqlMatchers)
	baseStatement := BaseStatement{SQL: query}
	result := &QueryStatement{BaseStatement: baseStatement}
	var token *toolbox.Token

	_, err := qp.expectOptionalWhitespaceFollowedBy(tokenizer, "SELECT", selectKeyword)
	if err != nil {
		return nil, err
	}

	token, err = qp.expectWhitespaceFollowedBy(tokenizer, "* | column | expression ", asterisk, id, expression)
	if err != nil {
		return nil, err
	}

	switch token.Token {
	case asterisk:
		result.AllField = true
		token, err = qp.expectWhitespaceFollowedBy(tokenizer, "FROM", fromKeyword)
		if err != nil {
			return nil, err
		}

	case id, expression:
		err = qp.readQueryColumns(tokenizer, result, &result.Columns, token, false, fromKeyword)
		if err != nil {
			return nil, err
		}

	}
	token, err = qp.expectWhitespaceFollowedBy(tokenizer, "table ", id)
	if err != nil {
		return nil, err
	}
	result.Table = token.Matched

	token, err = qp.expectOptionalWhitespaceFollowedBy(tokenizer, "WHERE | GROUP BY | eof", eof, whereKeyword, groupKeyword)
	if err != nil {
		return nil, err
	}
	if token.Token == eof {
		return result, nil
	}

	if token.Token == whereKeyword {
		err = qp.readCriteria(tokenizer, &result.BaseStatement, token)
		if err != nil {
			return nil, err
		}
		token, err = qp.expectOptionalWhitespaceFollowedBy(tokenizer, "WHERE | eof", eof, groupKeyword)
		if err != nil {
			return nil, err
		}
		if token.Token == eof {
			return result, nil
		}
	}

	if token.Token == groupKeyword {
		token, err = qp.expectOptionalWhitespaceFollowedBy(tokenizer, "BY", byKeyword)
		if err != nil {
			return nil, err
		}
		token, err = qp.expectWhitespaceFollowedBy(tokenizer, "column | expression ", id, expression, columnRef)
		if err != nil {
			return nil, err
		}

		err = qp.readQueryColumns(tokenizer, result, &result.GroupBy, token, true, eof)
		if err != nil {
			return nil, err
		}

	}

	return result, nil
}

//NewQueryParser represents basic SQL query parser.
func NewQueryParser() *QueryParser {
	return &QueryParser{}
}

//DmlParser represents dml parser.
type DmlParser struct{ baseParser }

func (dp *DmlParser) readInsertColumns(tokenizer *toolbox.Tokenizer, statement *DmlStatement) error {
	token, err := dp.expectOptionalWhitespaceFollowedBy(tokenizer, "(", groupingBegin)
	if err != nil {
		return err
	}
	statement.Columns = make([]*SQLColumn, 0)
	for i := tokenizer.Index; i < len(tokenizer.Input); i++ {
		token, err = dp.expectOptionalWhitespaceFollowedBy(tokenizer, "column", id)
		if err != nil {
			return err
		}
		field := &SQLColumn{Name: token.Matched}
		statement.Columns = append(statement.Columns, field)
		token, err = dp.expectOptionalWhitespaceFollowedBy(tokenizer, "','", coma, groupingEnd)
		if err != nil {
			return err
		}
		if token.Token == groupingEnd {
			return nil
		}
	}
	return nil
}

func (dp *DmlParser) readInsertValues(tokenizer *toolbox.Tokenizer, statement *DmlStatement) error {
	token, err := dp.expectOptionalWhitespaceFollowedBy(tokenizer, "(", groupingBegin)
	if err != nil {
		return err
	}
	statement.Values = make([]interface{}, 0)
	for i := tokenizer.Index; i < len(tokenizer.Input); i++ {
		token, err = dp.expectOptionalWhitespaceFollowedBy(tokenizer, "value", sqlValue)
		if err != nil {
			return err
		}
		statement.Values = append(statement.Values, token.Matched)
		token, err = dp.expectOptionalWhitespaceFollowedBy(tokenizer, "','", coma, groupingEnd)
		if err != nil {
			return err
		}
		if token.Token == groupingEnd {
			return nil
		}
	}
	return nil
}

func (dp *DmlParser) parseInsert(tokenizer *toolbox.Tokenizer, statement *DmlStatement) error {
	token, err := dp.expectWhitespaceFollowedBy(tokenizer, "INTO", intoKeyword)
	if err != nil {
		return err
	}
	token, err = dp.expectWhitespaceFollowedBy(tokenizer, "table", id)
	if err != nil {
		return err
	}
	statement.Table = token.Matched

	err = dp.readInsertColumns(tokenizer, statement)
	if err != nil {
		return err
	}

	_, err = dp.expectOptionalWhitespaceFollowedBy(tokenizer, "VALUES", valuesKeyword)
	if err != nil {
		return err
	}
	err = dp.readInsertValues(tokenizer, statement)
	if err != nil {
		return err
	}
	return nil
}

func (dp *DmlParser) readColumnAndValues(tokenizer *toolbox.Tokenizer, statement *DmlStatement) (*toolbox.Token, error) {
	statement.Columns = make([]*SQLColumn, 0)
	statement.Values = make([]interface{}, 0)
	hasColumn := false
	for i := tokenizer.Index; i < len(tokenizer.Input); i++ {
		token, err := dp.expectWhitespaceFollowedBy(tokenizer, "column", id)
		if err != nil {
			return nil, err
		}
		if token.Token == eof {
			break
		}
		hasColumn = true
		column := &SQLColumn{Name: token.Matched}
		statement.Columns = append(statement.Columns, column)
		_, err = dp.expectWhitespaceFollowedBy(tokenizer, "=", equalOperator)
		if err != nil {
			return nil, err
		}
		if token.Token == eof {
			return nil, newIllegalTokenParsingError(tokenizer.Index, "expected =")
		}

		token, err = dp.expectWhitespaceFollowedBy(tokenizer, "value", sqlValue)
		if err != nil {
			return nil, err
		}
		if token.Token == eof {
			return nil, newIllegalTokenParsingError(tokenizer.Index, "expected value")
		}
		statement.Values = append(statement.Values, token.Matched)
		token, err = dp.expectOptionalWhitespaceFollowedBy(tokenizer, ",| where | eof", eof, coma, whereKeyword)
		if err != nil {
			return nil, err
		}
		if token.Token != coma {
			return token, nil
		}
	}
	if !hasColumn {
		return nil, newIllegalTokenParsingError(tokenizer.Index, "expected column")
	}
	return &toolbox.Token{Token: eof}, nil
}

func (dp *DmlParser) parseUpdate(tokenizer *toolbox.Tokenizer, statement *DmlStatement) error {
	token, err := dp.expectWhitespaceFollowedBy(tokenizer, "table", id)
	if err != nil {
		return err
	}
	statement.Table = token.Matched
	_, err = dp.expectWhitespaceFollowedBy(tokenizer, "set", setKeyword)
	if err != nil {
		return err
	}

	token, err = dp.readColumnAndValues(tokenizer, statement)
	if err != nil {
		return err
	}

	if token.Token == eof {
		return nil
	}

	err = dp.readCriteria(tokenizer, &statement.BaseStatement, token)
	if err != nil {
		return err
	}
	return nil
}

func (dp *DmlParser) parseDelete(tokenizer *toolbox.Tokenizer, statement *DmlStatement) error {
	token, err := dp.expectWhitespaceFollowedBy(tokenizer, "FROM", fromKeyword)
	if err != nil {
		return err
	}
	token, err = dp.expectWhitespaceFollowedBy(tokenizer, "table", id)
	if err != nil {
		return err
	}
	statement.Table = token.Matched

	token, err = dp.expectOptionalWhitespaceFollowedBy(tokenizer, "where | eof", whereKeyword, eof)
	if err != nil {
		return err
	}
	if token.Token == eof {
		return nil
	}

	err = dp.readCriteria(tokenizer, &statement.BaseStatement, token)
	if err != nil {
		return err
	}
	return nil
}

//Parse parses input to create DmlStatement.
func (dp *DmlParser) Parse(input string) (*DmlStatement, error) {
	baseStatement := BaseStatement{SQL: input}
	result := &DmlStatement{BaseStatement: baseStatement}
	tokenizer := toolbox.NewTokenizer(input, illegal, eof, sqlMatchers)
	token, err := dp.expectOptionalWhitespaceFollowedBy(tokenizer, "INSERT INTO | UPDATE | DELETE", insertKeyword, updateKeyword, deleteKeyword)
	if err != nil {
		return nil, err
	}
	result.Type = token.Matched
	switch token.Token {
	case insertKeyword:
		err = dp.parseInsert(tokenizer, result)
	case updateKeyword:
		err = dp.parseUpdate(tokenizer, result)
	case deleteKeyword:
		err = dp.parseDelete(tokenizer, result)
	}
	if err != nil {
		return nil, err
	}
	return result, nil
}

//NewDmlParser creates a new NewDmlParser
func NewDmlParser() *DmlParser {
	return &DmlParser{}
}

type illegalTokenParsingError struct {
	Index    int
	Expected string
	error    string
}

func (e illegalTokenParsingError) Error() string {
	return e.error
}

func newIllegalTokenParsingError(index int, expected string) error {
	return &illegalTokenParsingError{Index: index, Expected: expected, error: fmt.Sprintf("Illegal token at %v, expected %v", index, expected)}
}
