package dsc

import "github.com/viant/toolbox"

type valueMatcher struct {
	optionallyEnclosingChar string
	terminatorChars         string
}

func (m valueMatcher) Match(input string, offset int) (matched int) {
	var i = 0
	isValueEnclosed := false
	if input[offset:offset + 1] == m.optionallyEnclosingChar {
		isValueEnclosed = true
		i++
	}
	for ; i < len(input) - offset; i++ {
		aChar := input[offset + i: offset + i + 1]
		if isValueEnclosed {
			if aChar == m.optionallyEnclosingChar && input[offset + i - 1: offset + i] != "\\" {
				i++
				break
			}

		} else {
			for j := 0; j < len(m.terminatorChars); j++ {
				if aChar == m.terminatorChars[j:j + 1] {
					return i
				}
			}
		}
	}

	if isValueEnclosed {
		if input[offset + i - 1: offset + i] == m.optionallyEnclosingChar {
			return i
		}
		return 0
	}

	return i
}


type valuesMatcher struct {
	valuesGroupingBeginChar         string
	valuesGroupingEndChar           string
	valueSeparator                  string
	valueOptionallyEnclosedWithChar string
	valueTerminatorCharacters       string
}


func (m valuesMatcher) Match(input string, offset int) (matched int) {
	if input[offset:offset + len(m.valuesGroupingBeginChar)] != m.valuesGroupingBeginChar {
		return 0
	}
	valueMatcher := valueMatcher{optionallyEnclosingChar:m.valueOptionallyEnclosedWithChar, terminatorChars:m.valueTerminatorCharacters}
	whitespaceMatcher := toolbox.CharactersMatcher{Chars:" \n\t"}


	i := len(m.valuesGroupingBeginChar)
	var firstIteration = true
	//"a(1, 2, 3)a"
	var maxLoopCount = len(input) - (offset + 1)
	for ; i < maxLoopCount; firstIteration=false {
		aChar := input[offset + i:offset + i + 1]
		if aChar == m.valueSeparator {
			if firstIteration {
				return 0
			}
			i++
			continue
		}
		whitespaceMatched := whitespaceMatcher.Match(input, offset + i)
		if whitespaceMatched > 0 {
			i += whitespaceMatched
			continue
		}

		valueMatched := valueMatcher.Match(input, offset + i)
		if valueMatched == 0 {
			if firstIteration {
				return 0
			}
			break
		}
		i += valueMatched

	}
	if offset + i < len(input) && input[offset+ i: offset + i + 1]!= m.valuesGroupingEndChar {
		return 0
	}
	return i + 1
}

