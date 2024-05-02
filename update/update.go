package update

import "fmt"

func Update() {
	var db string
	var record string
	var prevVal string
	var newVal string

	fmt.Print("Enter the database you want to update: ")
	fmt.Scanln(&db)

	fmt.Print("Enter the record you want to update: ")
	fmt.Scanln(&record)

	fmt.Print("What was the previous value: ")
	fmt.Scanln(&prevVal)

	fmt.Print("Enter the new value: ")
	fmt.Scanln(&newVal)

}
