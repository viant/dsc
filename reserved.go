package dsc

import (
	"os"
	"strings"
)

var reservedKeyword = map[string]bool{
	"key":        true,
	"primary":    true,
	"select":     true,
	"from":       true,
	"in":         true,
	"table":      true,
	"column":     true,
	"constraint": true,
	"foreign":    true,
	"index":      true,
	"all":        true,
	"and":        true,
	"or":         true,
	"as":         true,
	"asc":        true,
	"desc":       true,
	"begin":      true,
	"break":      true,
	"between":    true,
	"by":         true,
	"order":      true,
	"is":         true,
	"database":   true,
}

func updateReserved(pk []string) {
	if os.Getenv("SQLQuoteReserved") == "" {
		return
	}
	for i := range pk {
		if reservedKeyword[strings.ToLower(pk[i])] {
			if strings.Count(pk[i], "`") == 0 {
				pk[i] = "`" + pk[i] + "`"
			}
		}
	}
}
