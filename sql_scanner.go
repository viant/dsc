package dsc

import "github.com/viant/toolbox"

type SQLScanner struct {
	query     *QueryStatement
	columns   []string
	converter toolbox.Converter
	Values    map[string]interface{}
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


func NewSQLScanner(query *QueryStatement, config *Config, columns []string) *SQLScanner {
	converter := *toolbox.NewColumnConverter(config.GetDateLayout())
	if len(columns) == 0 {
		columns = query.ColumnNames()
	}
	return &SQLScanner{
		query:     query,
		columns:   columns,
		converter: converter,
	}
}

