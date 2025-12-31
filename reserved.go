package dsc

import (
	"strings"
)

// reservedKeyword holds the effective set of reserved identifiers that require quoting.
// It is initialized with a sensible default and can be extended via Config using InitReserved.
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

// quoteReservedEnabled controls whether quoting is applied to identifiers in updateReserved.
// It is configured exclusively via Config using InitReserved.
var quoteReservedEnabled = false

// InitReserved extends the reserved keywords set and toggles quoting based on config.
// It is safe to call multiple times; new keywords are merged into the existing set.
func InitReserved(config *Config) {
	if config == nil {
		return
	}
	// Determine enablement: prefer explicit boolean field, then parameters key.
	enabled := config.QuoteReserved
	if config.Has("quoteReserved") {
		enabled = config.GetBoolean("quoteReserved", enabled)
	}

	// Merge configured keywords. Priority: explicit field -> parameters["reservedKeywords"] -> parameters["reserved"].
	var list []string
	if len(config.ReservedKeywords) > 0 {
		list = append(list, config.ReservedKeywords...)
	} else {
		// Try parameters as comma/space separated or []any
		if raw := config.Get("reservedKeywords"); raw != "" {
			list = append(list, splitCSV(raw)...)
		} else if raw := config.Get("reserved"); raw != "" {
			list = append(list, splitCSV(raw)...)
		}
	}
	// If a custom list is provided and quoting wasn't explicitly disabled, enable quoting.
	if !enabled && len(list) > 0 {
		enabled = true
	}
	quoteReservedEnabled = enabled
	for _, k := range list {
		k = strings.TrimSpace(strings.ToLower(k))
		if k == "" {
			continue
		}
		reservedKeyword[k] = true
	}
}

// splitCSV splits a comma/space separated list into items.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	// Replace commas with spaces, then split on spaces to be flexible.
	s = strings.ReplaceAll(s, ",", " ")
	fields := strings.Fields(s)
	return fields
}

func updateReserved(pk []string) {
	if !quoteReservedEnabled {
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
