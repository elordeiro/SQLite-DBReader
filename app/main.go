package main

import (
	"fmt"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		rootPage := ReadDatabaseFile(databaseFilePath)

		fmt.Printf("database page size: %v\n", rootPage.Size)
		fmt.Printf("number of tables: %v\n", GetTableCount(rootPage))
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
