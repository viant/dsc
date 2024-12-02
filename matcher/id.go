package matcher

import "unicode"

var dotRune = rune('.')
var underscoreRune = rune('_')

// LiteralMatcher represents a matcher that finds any literals in the input
type IdMatcher struct{}

// Match matches a literal in the input, it returns number of character matched.
func (m IdMatcher) Match(input string, offset int) int {
	var matched = 0
	if offset >= len(input) {
		return matched
	}
	for i, r := range input[offset:] {
		if i == 0 {
			if !(unicode.IsLetter(r) || unicode.IsDigit(r)) {
				break
			}
		} else if !(unicode.IsLetter(r) || r == '-' || unicode.IsDigit(r) || r == dotRune || r == underscoreRune) {
			break
		}
		matched++
	}
	return matched
}
