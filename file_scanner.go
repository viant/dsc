package dsc

import (
	"encoding/json"
	"github.com/viant/toolbox"
	"strings"
)

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
func (s *FileScanner) Scan(destinations ...interface{}) (err error) {
	if len(destinations) == 1 {
		if toolbox.IsMap(destinations[0]){
			var record = toolbox.AsMap(destinations[0])
			for k, v := range s.Values {
				record[k]=v
			}
			return nil
		}
	}

	var columns, _ = s.Columns()
	for i, dest := range destinations {
		if value, found := s.Values[columns[i]]; found {
			switch val := value.(type) {
			case json.Number:
				var number interface{}
				if strings.Contains(val.String(), ".") {

					number, err = val.Float64()
					if err == nil {
						err = s.converter.AssignConverted(dest, number)
					}
				} else {
					number, err = val.Int64()
					if err == nil {
						err = s.converter.AssignConverted(dest, number)
					}
				}
				break

			default:
				err = s.converter.AssignConverted(dest, value)

			}
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
