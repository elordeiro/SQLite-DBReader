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

// Schema Constants -----------------------------------------------------------
/*
CREATE TABLE sqlite_schema(
	type text,        -> Idx 0
	name text,        -> Idx 1
	tbl_name text,    -> Idx 2
	rootpage integer, -> Idx 3
	sql text          -> Idx 4
);
*/

const (
	SchemaTypeIdx     = 0
	SchemaNameIdx     = 1
	SchemaRootPageIdx = 3
	SchemaTextIdx     = 4
)

// ----------------------------------------------------------------------------

// Custom Types ---------------------------------------------------------------
type SQLite struct {
	file     *os.File
	pageSize int64
	tables   []*Table
}

type Table struct {
	Type     int
	Name     string
	PageNum  int64
	ColNames []string
}

// ----------------------------------------------------------------------------

// Filters---------------------------------------------------------------------
type Filter interface {
	FilterCell(any) bool
}

type TableFilter struct {
	rowIds *[]uint64
}

type IndexFilter struct {
	key string
}

type NilFilter struct{}

func (tb TableFilter) FilterCell(a any) bool {
	cellRowID := any(a).(uint64)
	for _, id := range *tb.rowIds {
		if id <= cellRowID {
			return true
		}
	}
	return false
}

func (tb IndexFilter) PassesFilter(a any) bool {
	key := any(a).(string)
	return tb.key == key
}

func (tb NilFilter) FilterCell(a any) bool { return true }

// ----------------------------------------------------------------------------

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
		payloadSize, n := parseVarInt(pageBuf[off:])
		off += n

		// Read row ID
		_, n = parseVarInt(pageBuf[off:])
		off += n

		// Read Record
		cell.Record = ReadRecord(pageBuf[off : off+int(payloadSize)])

		// Append cell record to tables
		tables = append(tables, &Table{
			Type:     parseTableType(cell.Record.Keys[SchemaTypeIdx]),
			Name:     string(cell.Record.Keys[SchemaNameIdx]),
			PageNum:  int64(bytesToInt(cell.Record.Keys[SchemaRootPageIdx])),
			ColNames: parseColNames(cell.Record.Keys[SchemaTextIdx]),
		})

	}

	return tables
}

func (db *SQLite) ParseTablePage(pageNum int64, filter Filter) *Page {
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

	if header.Type == LeafTablePage {
		page.ParseLeafTableCells(pageBuf, filter)
		return page
	}

	page.ParseInteriorTableCells(pageBuf, filter)

	for _, cell := range page.Cells {
		childPage := db.ParseTablePage(int64(cell.LeftChildPointer), filter)
		page.Pages = append(page.Pages, childPage)
		if f, ok := filter.(TableFilter); ok && len(*f.rowIds) == 0 {
			break
		}
	}

	return page
}

func (db *SQLite) ParseIndexPage(pageNum int64, filter IndexFilter) *Page {
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

	if header.Type == LeafIndexPage {
		page.ParseLeafIndexCells(pageBuf, filter)
		return page
	}

	page.ParseInteriorIndexCells(pageBuf, filter)

	for _, cell := range page.Cells {
		childPage := db.ParseIndexPage(int64(cell.LeftChildPointer), filter)
		page.Pages = append(page.Pages, childPage)
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
		newPage = db.ParseTablePage(pageNum, nf)
		return newPage, nil
	}

	switch f := any(filter[0]).(type) {
	case string:
		newPage = db.ParseIndexPage(pageNum, IndexFilter{f})
	case []uint64:
		slices.Sort(f)
		newPage = db.ParseTablePage(pageNum, TableFilter{&f})
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
