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

	db := NewSQLite(databaseFilePath)

	switch command {
	case ".dbinfo":
		fmt.Printf("database page size: %v\n", db.GetPageSize())
		fmt.Printf("number of tables: %v\n", db.GetTableCount())
	case ".tables":
		tables := db.GetTableNames()
		for _, table := range tables {
			if strings.Contains(table, "sqlite_") {
				continue
			}
			fmt.Printf("%v ", table)
		}
		fmt.Println()
	default:
		result, err := HandleCommand(command, db)
		if err != nil {
			log.Fatal(err)
		}
		for _, res := range result {
			fmt.Println(res)
		}
	}

	db.file.Close()
}
