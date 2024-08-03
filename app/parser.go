package main

import (
	"strconv"
	"strings"
)

func (page *Page) ParseCommand(input string) string {
	// Testing getting row count from table
	parts := strings.Split(input, " ")
	pageName := parts[len(parts)-1]

	tableNames := GetTableNames(page)
	for i, tableName := range tableNames {
		if tableName == pageName {
			return strconv.Itoa(GetTableRowCount(i, page))
		}
	}
	return ""
}
