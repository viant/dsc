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
			columns, err :=  s.Columns();
			if err != nil {
				return err
			}
			destinations = make([]interface{}, len(columns))
			for i, column := range columns {
				var value interface{}
				aMap[column] = &value
				destinations[i] = &value
			}
		}
		if aMap, ok := destinations[0].(*map[string]interface{}); ok {
			columns, err :=  s.Columns();
			if err != nil {
				return err
			}
			destinations = make([]interface{}, len(columns))
			for i, column := range columns {
				var value interface{}
				(*aMap)[column] =  &value
				destinations[i] = &value
			}
		}
	}
	err :=  s.scanner.Scan(destinations...)
	return err
}

func NewScanner(s Scanner) Scanner {
	return &scanner{s}
}