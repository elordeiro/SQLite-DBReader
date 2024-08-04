package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
)

const (
	InteriorIndexBTreePage = 0x02
	InteriorTableBTreePage = 0x05
	LeafIndexBTreePage     = 0x0a
	LeafTableBTreePage     = 0x0d
)

type Page struct {
	Header   *Header
	Size     uint16
	Offset   int64
	PageNum  uint64
	CellPtrs []int16
	Cells    []*Cell
	Pages    []*Page
	DbFile   *os.File
}

type Header struct {
	Raw              []byte
	Type             uint8
	CellCount        uint16
	RightMostPointer uint32 // Only for interior table b-tree and interior index b-tree
}

type Cell struct {
	LeftChildPointer uint32 // Used for: TableInteriorCell, IndexInteriorCell
	RowID            uint64 // Used for: TableLeafCell, TableInteriorCell
	PayloadSize      uint64 // Used for: TableLeafCell, IndexLeafCell, IndexInteriorCell
	Payload          []byte // Used for: TableLeafCell, IndexLeafCell, IndexInteriorCell
	Record           *Record
}

type Record struct {
	HeaderSize  uint64    // Part of Header
	ColumnTypes []uint64  // Part of Header
	Keys        [][]uint8 // Part of Body
	RowID       int64     // Part of Body -- Not present in table leaf cells
}

func NewHeader(pageType uint8) *Header {
	var headerSize int
	if pageType == InteriorTableBTreePage || pageType == InteriorIndexBTreePage {
		headerSize = 12
	} else {
		headerSize = 8
	}

	header := &Header{
		Type: pageType,
		Raw:  make([]byte, headerSize),
	}

	return header
}

func NewPage(size uint16, offset int64, pageNum uint64, header *Header) *Page {
	page := &Page{
		Size:    size,
		Offset:  offset,
		PageNum: pageNum,
		Header:  header,
	}

	return page
}

func ReadDatabaseFile(databaseFilePath string) *Page {
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
		fmt.Println("Failed to read integer:", err)
		return nil
	}

	// Read database size
	var dbsize uint32
	err = binary.Read(bytes.NewReader(header[28:32]), binary.BigEndian, &dbsize)
	if err != nil {
		fmt.Println("Failed to read integer:", err)
		return nil
	}

	rootPage := TraverseBTree(databaseFile, pageSize, 0)
	return rootPage
}

func TraverseBTree(databaseFile *os.File, pageSize uint16, pageNumber uint64) *Page {
	offset := int64(pageNumber) * int64(pageSize)
	if pageNumber == 0 {
		offset = 100
	}

	header := ReadHeader(offset, databaseFile)
	page := NewPage(pageSize, offset, pageNumber, header)
	page.DbFile = databaseFile

	err := ReadCellPtrs(page)
	if err != nil {
		log.Fatal(err)
	}

	if page.Header.Type == LeafTableBTreePage || page.Header.Type == LeafIndexBTreePage {
		return page
	}

	for _, cell := range page.Cells {
		pageNumber := binary.BigEndian.Uint64(cell.Payload)
		page.Pages = append(page.Pages, TraverseBTree(databaseFile, pageSize, pageNumber))
	}

	return page
}

func ReadHeader(offset int64, file *os.File) *Header {
	// Read page header
	pageType := make([]byte, 1)
	_, err := file.ReadAt(pageType, offset)
	if err != nil {
		log.Fatal(err)
	}

	header := NewHeader(pageType[0])

	_, err = file.ReadAt(header.Raw, offset)
	if err != nil {
		log.Fatal(err)
	}

	// Read cell count
	err = binary.Read(bytes.NewReader(header.Raw[3:5]), binary.BigEndian, &header.CellCount)
	if err != nil {
		log.Fatal(err)
	}

	// Read right most pointer
	if header.Type == InteriorTableBTreePage || header.Type == InteriorIndexBTreePage {
		err = binary.Read(bytes.NewReader(header.Raw[8:12]), binary.BigEndian, &header.RightMostPointer)
		if err != nil {
			log.Fatal(err)
		}
	}

	return header
}

func ReadCellPtrs(page *Page) error {
	// Read cell pointers
	page.CellPtrs = make([]int16, page.Header.CellCount)
	var arrayStart int
	if page.Header.Type == InteriorTableBTreePage || page.Header.Type == InteriorIndexBTreePage {
		arrayStart = int(page.Offset) + 12
	} else {
		arrayStart = int(page.Offset) + 8
	}

	page.DbFile.Seek(int64(arrayStart), 0)
	for i := 0; i < int(page.Header.CellCount); i++ {
		var cellPtr int16
		err := binary.Read(page.DbFile, binary.BigEndian, &cellPtr)
		if err != nil {
			fmt.Println("Failed to read integer:", err)
			return err
		}
		page.CellPtrs[i] = cellPtr
	}

	// Read cells
	var err error
	switch page.Header.Type {
	case LeafTableBTreePage:
		err = ReadTableLeafCell(page)
	case LeafIndexBTreePage:
		err = ReadIndexLeafCell(page)
	case InteriorIndexBTreePage:
		err = ReadIndexInteriorCell(page)
	case InteriorTableBTreePage:
		err = ReadTableInteriorCell(page)
	}
	if err != nil {
		return err
	}

	return nil
}

/*
Table B-Tree Leaf Cell (header 0x0d):

	A varint which is the total number of bytes of payload, including any overflow
	A varint which is the integer key, a.k.a. "rowid"
	The initial portion of the payload that does not spill to overflow pages.
	A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
*/
func ReadTableLeafCell(page *Page) error {
	var offset int64
	if page.PageNum == 0 {
		offset = 0
	} else {
		offset = page.Offset
	}
	for i := 0; i < int(page.Header.CellCount); i++ {
		cellPtr := page.CellPtrs[i] + int16(offset)
		buf := make([]byte, int(page.Size)-(int(cellPtr)%int(page.Size)))
		page.DbFile.ReadAt(buf, int64(cellPtr))

		cell := &Cell{}

		// Read payload size
		payloadSize, n := binary.Uvarint(buf)
		cell.PayloadSize = payloadSize

		// Read row ID
		rowID, n1 := binary.Uvarint(buf[n:])
		cell.RowID = rowID

		// Read payload
		cell.Payload = make([]byte, cell.PayloadSize)
		page.DbFile.ReadAt(cell.Payload, int64(cellPtr)+int64(n+n1))

		// Append cell to page cells
		page.Cells = append(page.Cells, cell)
	}

	return nil
}

/*
Index B-Tree Leaf Cell (header 0x0a):

	A varint which is the total number of bytes of key payload, including any overflow
	The initial portion of the payload that does not spill to overflow pages.
	A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
*/
func ReadIndexLeafCell(page *Page) error {
	for i := 0; i < int(page.Header.CellCount); i++ {
		cellPtr := page.CellPtrs[i]
		buf := make([]byte, int(page.Size)-int(cellPtr))
		page.DbFile.ReadAt(buf, int64(cellPtr))

		cell := &Cell{}

		// Read payload size
		payloadSize, n := binary.Uvarint(buf)
		cell.PayloadSize = payloadSize

		// Read payload
		cell.Payload = make([]byte, cell.PayloadSize)
		page.DbFile.ReadAt(cell.Payload, int64(cellPtr)+int64(n))

		// Append cell to page cells
		page.Cells = append(page.Cells, cell)
	}

	return nil
}

/*
Index B-Tree Interior Cell (header 0x02):

	A 4-byte big-endian page number which is the left child pointer.
	A varint which is the total number of bytes of key payload, including any overflow
	The initial portion of the payload that does not spill to overflow pages.
	A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
*/
func ReadIndexInteriorCell(page *Page) error {
	for i := 0; i < int(page.Header.CellCount); i++ {
		cellPtr := page.CellPtrs[i]
		buf := make([]byte, int(page.Size)-int(cellPtr))
		page.DbFile.ReadAt(buf, int64(cellPtr))

		cell := &Cell{}

		// Read left child pointer
		err := binary.Read(bytes.NewReader(buf), binary.BigEndian, &cell.LeftChildPointer)
		if err != nil {
			fmt.Println("Failed to read integer:", err)
			return err
		}

		// Read payload size
		payloadSize, n := binary.Uvarint(buf[4:])
		cell.PayloadSize = payloadSize

		// Read payload
		cell.Payload = make([]byte, cell.PayloadSize)
		page.DbFile.ReadAt(cell.Payload, int64(cellPtr)+int64(n+4))

		// Append cell to page cells
		page.Cells = append(page.Cells, cell)
	}

	return nil
}

/*
Table B-Tree Interior Cell (header 0x05):

	A 4-byte big-endian page number which is the left child pointer.
	A varint which is the integer key
*/
func ReadTableInteriorCell(page *Page) error {
	for i := 0; i < int(page.Header.CellCount); i++ {
		cellPtr := page.CellPtrs[i]
		buf := make([]byte, int(page.Size)-int(cellPtr))
		page.DbFile.ReadAt(buf, int64(cellPtr))

		cell := &Cell{}

		// Read left child pointer
		err := binary.Read(bytes.NewReader(buf), binary.BigEndian, &cell.LeftChildPointer)
		if err != nil {
			fmt.Println("Failed to read integer:", err)
			return err
		}

		// Read row ID
		rowID, _ := binary.Uvarint(buf[4:])
		cell.RowID = rowID

		// Append cell to page cells
		page.Cells = append(page.Cells, cell)
	}

	return nil
}

func (rootPage *Page) GetPageSize() int {
	return int(rootPage.Size)
}

func (rootPage *Page) GetTableCount() int {
	if rootPage.Header.Type == LeafTableBTreePage {
		return len(rootPage.Cells)
	}

	var count int
	for _, childPage := range rootPage.Pages {
		count += childPage.GetTableCount()
	}

	return count
}

func (page *Page) GetTableNames() []string {
	if page.Header.Type == LeafTableBTreePage {
		var tableNames []string
		for _, cell := range page.Cells {
			payload := string(cell.Payload)
			idx := strings.Index(payload, "CREATE TABLE") + 13
			if idx != -1 {
				payload = strings.TrimSpace(payload[idx:strings.Index(payload, "(")])
				tableNames = append(tableNames, payload)
			}
		}

		return tableNames
	}

	var tableNames []string
	for _, childPage := range page.Pages {
		tableNames = append(tableNames, childPage.GetTableNames()...)
	}

	return tableNames
}

func (page *Page) GetTablePageNumber(name string) (uint64, error) {
	if page.Header.Type != LeafTableBTreePage {
		return 0, errors.New("not a table page")
	}

	var pageNum uint64
	tableNames := page.GetTableNames()
	for i, tableName := range tableNames {
		if strings.Trim(tableName, "\"") == name {
			pageNum = page.Cells[i].RowID
			break
		}
	}

	return pageNum, nil
}

func (page *Page) GetPageByName(name string) (*Page, error) {
	pageNum, err := page.GetTablePageNumber(name)
	if err != nil {
		return nil, err
	}

	offset := int64(pageNum) * int64(page.Size)
	header := ReadHeader(offset, page.DbFile)
	newPage := NewPage(page.Size, offset, pageNum, header)
	newPage.DbFile = page.DbFile

	return newPage, nil
}

func ReadRecord(numCols int, cell *Cell) error {
	record := &Record{
		Keys: make([][]uint8, 0),
	}

	// Read header size
	headerSize, n := binary.Uvarint(cell.Payload)
	record.HeaderSize = headerSize

	// Read column types
	for range numCols {
		colType, n1 := binary.Uvarint(cell.Payload[n:])
		record.ColumnTypes = append(record.ColumnTypes, colType)
		n += n1
	}

	// Read keys assuming they are strings
	for i := range numCols {
		if record.ColumnTypes[i] == 0 {
			record.Keys = append(record.Keys, []byte{0})
			continue
		}
		keyLen := (record.ColumnTypes[i] - 13) / 2
		record.Keys = append(record.Keys, cell.Payload[n:n+int(keyLen)])
		n += int(keyLen)
	}

	cell.Record = record
	return nil
}

func (page *Page) GetTableColumns(name string) ([]string, error) {
	if page.Header.Type != LeafTableBTreePage {
		return nil, errors.New("not a table page")
	}

	tableNames := page.GetTableNames()
	var rowNum int
	for i, tableName := range tableNames {
		if strings.Trim(tableName, "\"") == name {
			rowNum = i
			break
		}
	}

	payload := page.Cells[rowNum].Payload
	payloadStr := string(payload)

	startIdx := strings.Index(payloadStr, "(")
	if startIdx == -1 {
		return nil, errors.New("could not find columns")
	}

	columns := strings.Split(payloadStr[startIdx+1:strings.Index(payloadStr, ")")], ",")
	for i, column := range columns {
		columns[i] = strings.TrimSpace(column)
	}

	return columns, nil
}

func (page *Page) GetTablebyName(name string) (*Page, error) {
	if page.Header.Type != LeafTableBTreePage {
		return nil, errors.New("not a table page")
	}

	newPage, err := page.GetPageByName(name)
	if err != nil {
		return nil, err
	}

	err = ReadCellPtrs(newPage)
	if err != nil {
		return nil, err
	}

	columns, err := page.GetTableColumns(name)
	if err != nil {
		return nil, err
	}

	for _, cell := range newPage.Cells {
		err = ReadRecord(len(columns), cell)
		if err != nil {
			return nil, err
		}
	}

	return newPage, nil
}
