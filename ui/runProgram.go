package ui

import (
	"benchmarkDB/create"
	"benchmarkDB/delete"
	"benchmarkDB/read"
	"benchmarkDB/update"
	"fmt"
	"strings"
)

func runProgram(option string) {

	option = strings.ToLower(strings.ReplaceAll(option, " ", ""))

	switch option {
	case "create":
		create.Create()
	case "read":
		read.Read()
	case "update":
		update.Update()
	case "delete":
		delete.Delete()
	default:
		fmt.Printf("No program found for option: %s\n", option)
	}
}
