package dsc

import "github.com/viant/toolbox"

type SQLScanner struct {
	query       *QueryStatement
	columns     []string
	types       []ColumnType
	columnTypes []ColumnType
	converter   toolbox.Converter
	Values      map[string]interface{}
}

func (s *SQLScanner) ColumnTypes() ([]ColumnType, error) {
	return s.columnTypes, nil
}

func (s *SQLScanner) Columns() ([]string, error) {
	return s.columns, nil
}

func (s *SQLScanner) Scan(destinations ...interface{}) error {
	var columns, _ = s.Columns()
	if len(destinations) == 1 {
		if toolbox.IsMap(destinations[0]) {
			aMap := toolbox.AsMap(destinations[0])
			for k, v := range s.Values {
				aMap[k] = v
			}
			return nil
		}
	}
	for i, dest := range destinations {
		if dest == nil {
			continue
		}
		if value, found := s.Values[columns[i]]; found {
			err := s.converter.AssignConverted(dest, value)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//NewSQLScannerWithTypes create a scanner with type
func NewSQLScannerWithTypes(query *QueryStatement, config *Config, columns []string, types []ColumnType) *SQLScanner {
	converter := *toolbox.NewColumnConverter(config.GetDateLayout())
	if len(columns) == 0 {
		columns = query.ColumnNames()
	}

	return &SQLScanner{
		query:     query,
		types:     types,
		columns:   columns,
		converter: converter,
	}
}

//NewSQLScanner creates a new sql scanner
func NewSQLScanner(query *QueryStatement, config *Config, columns []string) *SQLScanner {
	return NewSQLScannerWithTypes(query, config, columns, make([]ColumnType, 0))
}
