package dsc

import (
	"errors"
	"fmt"
	"github.com/viant/toolbox"
	"strings"
)

func getOperandValue(operand interface{}, parameters toolbox.Iterator) (interface{}, error) {
	if operand != "?" {
		return operand, nil
	}
	var values = make([]interface{}, 1)
	if !parameters.HasNext() {
		return "", fmt.Errorf("missing binding parameters ?, %v", parameters)
	}
	err := parameters.Next(&values[0])
	if err != nil {
		return nil, err
	}
	return values[0], nil
}

func getOperandValues(operands []interface{}, parameters toolbox.Iterator) ([]interface{}, error) {
	var result = make([]interface{}, 0)
	for _, operand := range operands {
		operand, err := getOperandValue(operand, parameters)
		if err != nil {
			return nil, err
		}
		result = append(result, operand)
	}
	return result, nil
}

//NewSQLCriterionPredicate create a new predicate for passed in SQLCriterion
func NewSQLCriterionPredicate(criterion *SQLCriterion, parameters toolbox.Iterator) (toolbox.Predicate, error) {
	if criterion.Operator == "" {
		return nil, errors.New("criterion.Operator was empty")
	}
	switch strings.ToLower(criterion.Operator) {
	case "in":
		operands, err := getOperandValues(criterion.RightOperands, parameters)
		if err != nil {
			return nil, fmt.Errorf("missing binding parameters for %v", criterion)
		}
		return toolbox.NewInPredicate(operands...), nil
	case "like":
		operand, err := getOperandValue(criterion.RightOperand, parameters)
		if err != nil {
			return nil, fmt.Errorf("missing binding parameters for %v", criterion)
		}
		return toolbox.NewLikePredicate(toolbox.AsString(operand)), nil
	case "between":
		operands, err := getOperandValues(criterion.RightOperands, parameters)
		if err != nil || len(operands) != 2 {
			return nil, fmt.Errorf("missing binding parameters for %v", criterion)
		}
		return toolbox.NewBetweenPredicate(operands[0], operands[1]), nil
	case "is":
		return toolbox.NewNilPredicate(), nil
	default:
		operand, err := getOperandValue(criterion.RightOperand, parameters)
		if err != nil {
			return nil, fmt.Errorf("missing binding parameters for %v", criterion)
		}
		return toolbox.NewComparablePredicate(criterion.Operator, operand), nil
	}
}

type booleanPredicate struct {
	operator    string
	leftOperand bool
}

func (p *booleanPredicate) Apply(value interface{}) bool {
	rightOperand := toolbox.AsBoolean(value)
	switch strings.ToLower(p.operator) {
	case "or":
		return p.leftOperand || rightOperand
	case "and":
		return p.leftOperand && rightOperand
	}
	return false
}

//NewBooleanPredicate returns a new boolean predicate. It takes left operand and logical operator: 'OR' or 'AND'
func NewBooleanPredicate(leftOperand bool, operator string) toolbox.Predicate {
	return &booleanPredicate{operator, leftOperand}
}

type sqlCriteriaPredicate struct {
	*SQLCriteria
	predicates []toolbox.Predicate
}

func (p *sqlCriteriaPredicate) Apply(source interface{}) bool {
	var sourceMap, ok = source.(map[string]interface{})
	if !ok {
		return false
	}
	result := true
	var logicalPredicate toolbox.Predicate

	for i := 0; i < len(p.Criteria); i++ {
		criterion := p.Criteria[i]
		value := sourceMap[toolbox.AsString(criterion.LeftOperand)]
		predicate := p.predicates[i]
		result = predicate.Apply(value)
		if criterion.Inverse {
			result = !result
		}
		if logicalPredicate != nil {
			result = logicalPredicate.Apply(result)
		}
		if p.LogicalOperator != "" {
			if strings.ToLower(p.LogicalOperator) == "and" && !result {
				//shortcut
				break
			}
			logicalPredicate = NewBooleanPredicate(result, p.LogicalOperator)
		}
	}
	return result
}

//NewSQLCriteriaPredicate create a new sql criteria predicate, it takes binding parameters iterator, and actual criteria.
func NewSQLCriteriaPredicate(parameters toolbox.Iterator, sqlCriteria *SQLCriteria) (toolbox.Predicate, error) {
	var predicates = make([]toolbox.Predicate, 0)

	for i := 0; i < len(sqlCriteria.Criteria); i++ {
		criterion := sqlCriteria.Criteria[i]
		predicate, err := NewSQLCriterionPredicate(criterion, parameters)
		if err != nil {
			return nil, err
		}
		predicates = append(predicates, predicate)
	}
	return &sqlCriteriaPredicate{SQLCriteria: sqlCriteria, predicates: predicates}, nil
}
