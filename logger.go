package dsc

import "fmt"

//Log represent log function
type Log func(format string, args ...interface{})

//Logf - function to log debug info
var Logf Log = VoidLogger

//VoidLogger represent logger that do not log
func VoidLogger(format string, args ...interface{}) {

}

//StdoutLogger represents stdout logger
func StdoutLogger(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
