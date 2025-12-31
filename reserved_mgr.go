package dsc

import "strings"

// Reserved encapsulates per-manager reserved-identifier quoting settings.
type Reserved struct {
	enabled   bool
	quoteChar string
	keywords  map[string]bool
}

// newReservedFromConfig builds a Reserved settings instance from Config.
// - enablement sourced from Config.QuoteReserved or parameters["quoteReserved"].
// - keywords merged from Config.ReservedKeywords or parameters ["reservedKeywords"/"reserved"].
// - quoteChar can be extended later; default is backtick to preserve existing behavior.
func newReservedFromConfig(config *Config) *Reserved {
	r := &Reserved{enabled: false, quoteChar: "`", keywords: map[string]bool{}}
	if config == nil {
		return r
	}
	// start with defaults currently in reservedKeyword
	for k, v := range reservedKeyword {
		r.keywords[k] = v && true
	}
	enabled := config.QuoteReserved
	if config.Has("quoteReserved") {
		enabled = config.GetBoolean("quoteReserved", enabled)
	}
	// merge list
	var list []string
	if len(config.ReservedKeywords) > 0 {
		list = append(list, config.ReservedKeywords...)
	} else {
		if raw := config.Get("reservedKeywords"); raw != "" {
			list = append(list, splitCSV(raw)...)
		} else if raw := config.Get("reserved"); raw != "" {
			list = append(list, splitCSV(raw)...)
		}
	}
	if !enabled && len(list) > 0 {
		enabled = true
	}
	r.enabled = enabled
	for _, k := range list {
		if k == "" {
			continue
		}
		r.keywords[strings.ToLower(k)] = true
	}
	return r
}

// quoteIfReserved quotes identifier names in-place if they match the reserved list and quoting is enabled.
func (r *Reserved) quoteIfReserved(columns []string) {
	if r == nil || !r.enabled {
		return
	}
	for i := range columns {
		key := strings.ToLower(columns[i])
		if r.keywords[key] {
			if !hasQuotes(columns[i], r.quoteChar) {
				columns[i] = r.quoteChar + columns[i] + r.quoteChar
			}
		}
	}
}

func hasQuotes(s, q string) bool {
	if len(s) < 2 {
		return false
	}
	return s[0:1] == q && s[len(s)-1:] == q
}

// splitCSV splits a comma/space separated list into items.
func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	s = strings.ReplaceAll(s, ",", " ")
	return strings.Fields(s)
}
