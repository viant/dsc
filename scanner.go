package dsc

type scanner struct {
	scanner Scanner
}

func (s *scanner) Columns() ([]string, error) {
	return s.scanner.Columns()
}

func (s *scanner) Scan(destinations ...interface{}) error {

	if len(destinations) == 1 {

		if aMap, ok := destinations[0].(map[string]interface{}); ok {
			values, columns, err := ScanRow(s)
			if err != nil {
				return err
			}
			for i, column := range columns {
				aMap[column] = values[i]
			}
			return nil

		}
		if aMap, ok := destinations[0].(*map[string]interface{}); ok {
			values, columns, err := ScanRow(s)
			if err != nil {
				return err
			}
			for i, column := range columns {
				(*aMap)[column] = values[i]
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
