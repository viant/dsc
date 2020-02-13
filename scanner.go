package dsc

import "github.com/viant/toolbox"

type scanner struct {
	scanner Scanner
}

func (s *scanner) Columns() ([]string, error) {
	return s.scanner.Columns()
}

func (s *scanner) ColumnTypes() ([]ColumnType, error) {
	return s.scanner.ColumnTypes()
}

func (s *scanner) Scan(destinations ...interface{}) error {
	if len(destinations) == 1 {
		if toolbox.IsMap(destinations[0]) {
			aMap := toolbox.AsMap(destinations[0])
			values, columns, err := ScanRow(s)
			if err != nil {
				return err
			}
			for i, column := range columns {
				aMap[column] = values[i]
			}
			return nil
		}
	}
	err := s.scanner.Scan(destinations...)
	return err
}

func NewScanner(s Scanner) Scanner {
	return &scanner{s}
}
