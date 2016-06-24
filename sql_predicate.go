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

// Package dsc - Predicate
package dsc

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/viant/toolbox"
)

var trueProvider = func(input interface{}) bool {
	return true
}

type betweenPredicate struct {
	from float64
	to   float64
}

func (p *betweenPredicate) Apply(value interface{}) bool {
	floatValue := toolbox.AsFloat(value)
	return floatValue >= p.from && floatValue <= p.to
}

func (p *betweenPredicate) String() string {
	return fmt.Sprintf("x BETWEEN %v AND %v", p.from, p.to)
}

//NewBetweenPredicate creates a new sql BETWEEN predicate, it takes from and expected as number parameters.
func NewBetweenPredicate(from, to interface{}) toolbox.Predicate {
	var result toolbox.Predicate = &betweenPredicate{
		from: toolbox.AsFloat(from),
		to:   toolbox.AsFloat(to),
	}
	return result
}

type inPredicate struct {
	predicate toolbox.Predicate
}

func (p *inPredicate) Apply(value interface{}) bool {
	return p.predicate.Apply(value)
}

//NewInPredicate creates a new sql IN predicate
func NewInPredicate(values ...interface{}) toolbox.Predicate {
	converted, kind := toolbox.DiscoverCollectionValuesAndKind(values)
	switch kind {
	case reflect.Int:
		predicate := inIntPredicate{values: make(map[int]bool)}
		toolbox.SliceToMap(converted, predicate.values, func(item interface{}) int {
			return toolbox.AsInt(item)
		}, trueProvider)
		return &predicate
	case reflect.Float64:
		predicate := inFloatPredicate{values: make(map[float64]bool)}
		toolbox.SliceToMap(converted, predicate.values, func(item interface{}) float64 {
			return toolbox.AsFloat(item)
		}, trueProvider)
		return &predicate
	default:
		predicate := inStringPredicate{values: make(map[string]bool)}
		toolbox.SliceToMap(converted, predicate.values, func(item interface{}) string {
			return toolbox.AsString(item)
		}, trueProvider)
		return &predicate
	}
}

type inFloatPredicate struct {
	values map[float64]bool
}

func (p *inFloatPredicate) Apply(value interface{}) bool {
	candidate := toolbox.AsFloat(value)
	return p.values[candidate]
}

type inIntPredicate struct {
	values map[int]bool
}

func (p *inIntPredicate) Apply(value interface{}) bool {
	candidate := toolbox.AsInt(value)
	return p.values[int(candidate)]
}

type inStringPredicate struct {
	values map[string]bool
}

func (p *inStringPredicate) Apply(value interface{}) bool {
	candidate := toolbox.AsString(value)
	return p.values[candidate]
}

type numericComparablePredicate struct {
	rightOperand float64
	operator     string
}

func (p *numericComparablePredicate) Apply(value interface{}) bool {
	leftOperand := toolbox.AsFloat(value)
	switch p.operator {
	case ">":
		return leftOperand > p.rightOperand
	case ">=":
		return leftOperand >= p.rightOperand
	case "<":
		return leftOperand < p.rightOperand
	case "<=":
		return leftOperand <= p.rightOperand
	case "=":
		return leftOperand == p.rightOperand
	case "!=":
		return leftOperand != p.rightOperand
	}
	return false
}

type stringComparablePredicate struct {
	rightOperand string
	operator     string
}

func (p *stringComparablePredicate) Apply(value interface{}) bool {
	leftOperand := toolbox.AsString(value)

	switch p.operator {
	case "=":
		return leftOperand == p.rightOperand
	case "!=":
		return leftOperand != p.rightOperand
	}
	return false
}

//NewComparablePredicate create a new comparable predicate for =, !=, >=, <=
func NewComparablePredicate(operator string, leftOperand interface{}) toolbox.Predicate {
	if toolbox.CanConvertToFloat(leftOperand) {
		return &numericComparablePredicate{toolbox.AsFloat(leftOperand), operator}
	}
	return &stringComparablePredicate{toolbox.AsString(leftOperand), operator}
}

type nilPredicate struct{}

func (p *nilPredicate) Apply(value interface{}) bool {
	return value == nil || reflect.ValueOf(value).IsNil()
}

type likePredicate struct {
	matchingFragments []string
}

func (p *likePredicate) Apply(value interface{}) bool {
	textValue := strings.ToLower(toolbox.AsString(value))
	for _, matchingFragment := range p.matchingFragments {
		matchingIndex := strings.Index(textValue, matchingFragment)
		if matchingIndex == -1 {
			return false
		}
		if matchingIndex < len(textValue) {
			textValue = textValue[matchingIndex:]
		}
	}
	return true
}

//NewLikePredicate create a new like predicate
func NewLikePredicate(matching string) toolbox.Predicate {
	return &likePredicate{matchingFragments: strings.Split(strings.ToLower(matching), "%")}
}

func getOperandValue(operand interface{}, parameters toolbox.Iterator) (interface{}, error) {
	if operand != "?" {
		return operand, nil
	}
	var values = make([]interface{}, 1)
	if !parameters.HasNext() {
		return "", errors.New("Unable to expand ? - not more parameters")
	}
	parameters.Next(&values[0])
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
func NewSQLCriterionPredicate(criterion SQLCriterion, parameters toolbox.Iterator) (toolbox.Predicate, error) {
	if criterion.Operator == "" {
		return nil, errors.New("criterion.Operator was empty")
	}
	switch strings.ToLower(criterion.Operator) {
	case "in":
		operands, err := getOperandValues(criterion.RightOperands, parameters)
		if err != nil {
			return nil, fmt.Errorf("Not enough binding parameters for %v", criterion)
		}
		return NewInPredicate(operands...), nil
	case "like":
		operand, err := getOperandValue(criterion.RightOperand, parameters)
		if err != nil {
			return nil, fmt.Errorf("Not enough binding parameters for %v", criterion)
		}
		return NewLikePredicate(toolbox.AsString(operand)), nil
	case "between":
		operands, err := getOperandValues(criterion.RightOperands, parameters)
		if err != nil || len(operands) != 2 {
			return nil, fmt.Errorf("Not enough binding parameters for %v", criterion)
		}
		return NewBetweenPredicate(operands[0], operands[1]), nil
	case "is":
		return &nilPredicate{}, nil
	default:
		operand, err := getOperandValue(criterion.RightOperand, parameters)
		if err != nil {
			return nil, fmt.Errorf("Not enough binding parameters for %v", criterion)
		}
		return NewComparablePredicate(criterion.Operator, operand), nil
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
	criteria   []SQLCriterion
	predicates []toolbox.Predicate
}

func (p *sqlCriteriaPredicate) Apply(source interface{}) bool {
	var sourceMap, ok = source.(map[string]interface{})
	if !ok {
		return false
	}
	result := true
	var logicalPredicate toolbox.Predicate

	for i := 0; i < len(p.criteria); i++ {
		criterion := p.criteria[i]
		value := sourceMap[toolbox.AsString(criterion.LeftOperand)]
		predicate := p.predicates[i]
		result = predicate.Apply(value)
		if criterion.Inverse {
			result = !result
		}
		if logicalPredicate != nil {
			result = logicalPredicate.Apply(result)
		}
		if criterion.LogicalOperator != "" {
			if strings.ToLower(criterion.LogicalOperator) == "and" && !result {
				//shortcut
				break
			}
			logicalPredicate = NewBooleanPredicate(result, criterion.LogicalOperator)
		}
	}
	return result
}

//NewSQLCriteriaPredicate create a new sql criteria predicate, it takes binding parameters iterator, and actual criteria.
func NewSQLCriteriaPredicate(parameters toolbox.Iterator, criteria ...SQLCriterion) (toolbox.Predicate, error) {
	var predicates = make([]toolbox.Predicate, 0)
	for i := 0; i < len(criteria); i++ {
		criterion := criteria[i]
		predicate, err := NewSQLCriterionPredicate(criterion, parameters)
		if err != nil {
			return nil, err
		}
		predicates = append(predicates, predicate)
	}
	return &sqlCriteriaPredicate{criteria: criteria, predicates: predicates}, nil
}
