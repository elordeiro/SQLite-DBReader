package main

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	rootPage := ReadDatabaseFile(databaseFilePath)

	switch command {
	case ".dbinfo":
		fmt.Printf("database page size: %v\n", rootPage.GetPageSize())
		fmt.Printf("number of tables: %v\n", rootPage.GetTableCount())
	case ".tables":
		tables := rootPage.GetTableNames()
		for _, table := range tables {
			if strings.Contains(table, "sqlite_") {
				continue
			}
			fmt.Printf("%v ", table)
		}
		fmt.Println()
	default:
		result, err := rootPage.HandleCommand(command)
		if err != nil {
			log.Fatal(err)
		}
		for _, res := range result {
			fmt.Println(res)
		}
	}

	rootPage.DbFile.Close()
}
