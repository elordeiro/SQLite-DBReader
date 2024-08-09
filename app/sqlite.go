package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"log"
	"os"
	"slices"
)

const (
	MaxHeaderLen = 12
)

const (
	InteriorIndexPage = 0x02
	InteriorTablePage = 0x05
	LeafIndexPage     = 0x0a
	LeafTablePage     = 0x0d
)

const (
	TableTypeTable = iota
	TableTypeIndex
	TableTypeView
	TableTypeTrigger
)

const (
	SchemaTypeIdx     = 0
	SchemaNameIdx     = 1
	SchemaRootPageIdx = 3
	SchemaTextIdx     = 4
	IndexPageKeyIdx   = 0
	IndexPageRowIdIdx = 1
)

type SQLite struct {
	file     *os.File
	pageSize int64
	tables   []*Table
}

func NewSQLite(databaseFilePath string) *SQLite {
	// Open database file
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}

	// Read database file header
	header := make([]byte, 100)
	_, err = databaseFile.Read(header)
	if err != nil {
		log.Fatal(err)
	}

	// Read page size
	var pageSize uint16
	err = binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &pageSize)
	if err != nil {
		log.Fatal(err)
	}

	db := &SQLite{
		file:     databaseFile,
		pageSize: int64(pageSize),
	}

	db.tables = db.ParseSQLiteSchema()

	return db
}

func (db *SQLite) ParseSQLiteSchema() []*Table {
	// Load page into memory
	pageBuf := make([]byte, db.pageSize)
	db.file.ReadAt(pageBuf, 0)

	header := ParseHeader(pageBuf[100:108])
	cellPtrs := ParseCellPtrs(pageBuf[100:], header)

	tables := make([]*Table, 0)
	for i := 0; i < header.CellCount; i++ {
		off := cellPtrs[i]

		cell := &Cell{}

		// Read payload size
		payloadSize, n := ReadVarInt(pageBuf[off : off+MaxVarIntLen])
		off += n

		// Read row ID
		_, n = ReadVarInt(pageBuf[off : off+MaxVarIntLen])
		off += n

		// Read Record
		cell.Record = ReadRecord(pageBuf[off : off+int(payloadSize)])

		// Append cell record to tables
		tables = append(tables, &Table{
			Type:     readTableType(cell.Record.Keys[SchemaTypeIdx]),
			Name:     string(cell.Record.Keys[SchemaNameIdx]),
			PageNum:  int64(bytesToInt(cell.Record.Keys[SchemaRootPageIdx])),
			ColNames: parseColNames(cell.Record.Keys[SchemaTextIdx]),
		})

	}

	return tables
}

type NilFilter int

type Filter interface {
	string | *[]uint64 | NilFilter
}

func ParsePage[T Filter](db *SQLite, pageNum int64, filter T) *Page {
	// Load page into memory
	offset := db.calcOffset(pageNum)
	pageBuf := make([]byte, db.pageSize)
	db.file.ReadAt(pageBuf, offset)

	header := ParseHeader(pageBuf[0:MaxHeaderLen])
	cellPtrs := ParseCellPtrs(pageBuf, header)

	page := &Page{
		Header:   header,
		CellPtrs: cellPtrs,
	}

	switch header.Type {
	case LeafTablePage:
		switch f := any(filter).(type) {
		case NilFilter:
			page.ParseLeafTableCells(pageBuf)
		case *[]uint64:
			if len(*f) == 0 {
				return page
			}
			page.ParseLeafTableCellsFiltered(pageBuf, f)
		}
	case LeafIndexPage:
		page.ParseLeafIndexCells(pageBuf, any(filter).(string))
	case InteriorTablePage:
		switch f := any(filter).(type) {
		case NilFilter:
			page.ParseInteriorTableCells(pageBuf)
		case *[]uint64:
			if len(*f) == 0 {
				return page
			}
			page.ParseInteriorTableCellsFiltered(pageBuf, f)
		}
		page.Pages = make([]*Page, 0)
		for _, cell := range page.Cells {
			childPage := ParsePage(db, int64(cell.LeftChildPointer), filter)
			page.Pages = append(page.Pages, childPage)
			if f, ok := any(filter).(*[]uint64); ok && len(*f) == 0 {
				break
			}
		}
	case InteriorIndexPage:
		page.ParseInteriorIndexCells(pageBuf, any(filter).(string))
		page.Pages = make([]*Page, 0)
		for _, cell := range page.Cells {
			childPage := ParsePage(db, int64(cell.LeftChildPointer), filter)
			page.Pages = append(page.Pages, childPage)
		}
	}

	return page
}

// Helpers --------------------------------------------------------------------
func (db *SQLite) calcOffset(pageNum int64) int64 {
	if pageNum == 1 {
		return 100
	}
	return (pageNum - 1) * db.pageSize
}

// ----------------------------------------------------------------------------

// Getters --------------------------------------------------------------------
func (db *SQLite) GetPageSize() int {
	return int(db.pageSize)
}

func (db *SQLite) GetTableCount() int {
	return len(db.tables)
}

func (db *SQLite) GetTableNames() []string {
	var tableNames []string
	for _, table := range db.tables {
		tableNames = append(tableNames, table.Name)
	}

	return tableNames
}

func (db *SQLite) GetRootPageByName(name string, filter ...any) (*Page, error) {
	pageNum, err := db.getRootPageNumber(name)
	if err != nil {
		return nil, err
	}

	var newPage *Page
	if len(filter) == 0 {
		var nf NilFilter
		newPage = ParsePage(db, pageNum, nf)
		return newPage, nil
	}

	switch f := any(filter[0]).(type) {
	case string:
		newPage = ParsePage(db, pageNum, f)
	case []uint64:
		slices.Sort(f)
		newPage = ParsePage(db, pageNum, &f)
	default:
		return nil, errors.New("filter type must be string or []uint64")
	}

	return newPage, nil
}

func (db *SQLite) GetIndexPageName(key string) string {
	for _, table := range db.tables {
		if table.Type == TableTypeIndex && slices.Contains(table.ColNames, key) {
			return table.Name
		}
	}
	return ""
}

func (db *SQLite) GetTableColNames(name string) []string {
	tableNames := db.GetTableNames()

	var rowNum int
	for i, tableName := range tableNames {
		if tableName == name {
			rowNum = i
			break
		}
	}

	return db.tables[rowNum].ColNames
}

func (db *SQLite) GetFilteredRows(tableName string, rowIDs []uint64) [][]string {
	return nil
}

// ----------------------------------------------------------------------------

// Getter Helpers -------------------------------------------------------------
func (db *SQLite) getRootPageNumber(name string) (int64, error) {
	for _, table := range db.tables {
		if table.Name == name {
			return table.PageNum, nil
		}
	}

	return 0, errors.New("page name not found")
}

// ----------------------------------------------------------------------------
