package main

import (
	"fmt"
	"os"
	"strings"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	rootPage := ReadDatabaseFile(databaseFilePath)

	switch command {
	case ".dbinfo":
		fmt.Printf("database page size: %v\n", rootPage.Size)
		fmt.Printf("number of tables: %v\n", GetTableCount(rootPage))
	case ".tables":
		tables := GetTableNames(rootPage)
		for _, table := range tables {
			if strings.Contains(table, "sqlite_") {
				continue
			}
			fmt.Printf("%v ", table)
		}
		fmt.Println()
	default:
		fmt.Println(rootPage.ParseCommand(command))
	}

	rootPage.DbFile.Close()
}
