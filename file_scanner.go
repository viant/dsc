package dsc

import "github.com/viant/toolbox"

//FileScanner represents a file scanner to transfer record to destinations.
type FileScanner struct {
	columns   []string
	converter toolbox.Converter
	Values    map[string]interface{}
}

//Columns returns columns of the processed record.
func (s *FileScanner) Columns() ([]string, error) {
	return s.columns, nil
}

//Scan reads file record values to assign it to passed in destinations.
func (s *FileScanner) Scan(destinations ...interface{}) error {
	var columns, _ = s.Columns()
	for i, dest := range destinations {
		if value, found := s.Values[columns[i]]; found {
			err := s.converter.AssignConverted(dest, value)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//NewFileScanner create a new file scanner, it takes config, and columns as parameters.
func NewFileScanner(config *Config, columns []string) *FileScanner {
	converter := toolbox.NewColumnConverter(config.GetDateLayout())
	return &FileScanner{
		converter: *converter,
		columns:   columns,
	}
}
