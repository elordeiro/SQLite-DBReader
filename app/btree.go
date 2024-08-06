package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
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
	Tables   []*Table
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

type Table struct {
	Name     string
	PageNum  int
	ColNames []string
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

	rootPage := TraverseBTree(databaseFile, pageSize, 1)
	return rootPage
}

func TraverseBTree(databaseFile *os.File, pageSize uint16, pageNumber uint64) *Page {
	offset := int64(pageNumber-1) * int64(pageSize)
	if pageNumber == 1 {
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

	page.Pages = make([]*Page, 0)
	for _, cell := range page.Cells {
		childPage := TraverseBTree(databaseFile, pageSize, uint64(cell.LeftChildPointer))
		page.Pages = append(page.Pages, childPage)
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
	offset := page.Offset
	if page.PageNum == 1 {
		offset = 0
	}

	cellCount := int(page.Header.CellCount)
	for i := 0; i < cellCount; i++ {
		cellPtr := int64(page.CellPtrs[i]) + offset
		buf := make([]byte, int(page.Size)-(int(cellPtr)%int(page.Size)))
		page.DbFile.ReadAt(buf, int64(cellPtr))

		cell := &Cell{}

		// Read payload size
		payloadSize, n := binary.Uvarint(buf)
		cell.PayloadSize = payloadSize

		// Read row ID
		rowID, n1 := ReadVarInt(buf[n:])
		cell.RowID = rowID

		// Read Record
		buf = buf[n+n1:]
		cell.Record = ReadRecord(buf)

		for _, record := range cell.Record.Keys {
			if strings.Contains(string(record), "Stealth") {
				x := 0
				_ = x
			}
		}

		// Append cell to page cells
		page.Cells = append(page.Cells, cell)

		if page.PageNum == 1 {
			page.Tables = append(page.Tables, &Table{
				Name:     string(cell.Record.Keys[1]),
				PageNum:  bytesToInt(cell.Record.Keys[3]),
				ColNames: parseColNames(cell.Record.Keys[4]),
			})
		}
	}

	return nil
}

// Little-endian
// func ReadVarInt(buf []byte) (uint64, int) {
// 	result := uint64(0)
// 	for i, b := range buf {
// 		result |= uint64(b&0x7f) << uint(i*7)
// 		if b&0x80 == 0 {
// 			return result, i + 1
// 		}
// 	}
// 	return result, 0
// }

// Big-endian
func ReadVarInt(buf []byte) (uint64, int) {
	result := uint64(0)
	for i, b := range buf {
		result <<= 7
		result |= uint64(b & 0x7f)
		if b&0x80 == 0 {
			return result, i + 1
		}
	}
	return result, 0
}

func bytesToInt(bytes []byte) int {
	var result int
	for _, b := range bytes {
		result = (result << 8) | int(b)
	}
	return result
}

func parseColNames(bytes []byte) []string {
	exprStr := string(bytes)
	insideExpr := exprStr[strings.Index(exprStr, "(")+1 : strings.Index(exprStr, ")")]
	result := strings.Split(insideExpr, ",")
	for i, res := range result {
		result[i] = strings.TrimSpace(res)
	}
	return result
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
	offset := page.Offset

	for i := 0; i < int(page.Header.CellCount); i++ {
		cellPtr := int64(page.CellPtrs[i])
		buf := make([]byte, int(page.Size)-int(cellPtr))
		cellPtr += offset
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
		return len(rootPage.Tables)
	}

	var count int
	for _, childPage := range rootPage.Pages {
		count += childPage.GetTableCount()
	}

	return count
}

func (rootPage *Page) GetTableNames() ([]string, error) {
	if rootPage.Header.Type != LeafTableBTreePage {
		return nil, errors.New("page is not a root page")
	}

	var tableNames []string
	for _, table := range rootPage.Tables {
		tableNames = append(tableNames, table.Name)
	}

	return tableNames, nil
}

func (rootPage *Page) GetTablePageNumber(name string) (int, error) {
	if rootPage.Header.Type != LeafTableBTreePage {
		return 0, errors.New("page is not a root page")
	}

	for _, table := range rootPage.Tables {
		if table.Name == name {
			return table.PageNum, nil
		}
	}

	return -1, errors.New("page name not found")
}

func (page *Page) GetPageByName(name string) (*Page, error) {
	pageNum, err := page.GetTablePageNumber(name)
	if err != nil {
		return nil, err
	}

	newPage := TraverseBTree(page.DbFile, page.Size, uint64(pageNum))
	return newPage, nil
}

func ReadRecord(buf []byte) *Record {
	record := &Record{
		Keys: make([][]uint8, 0),
	}

	// Read header size
	headerSize, n := ReadVarInt(buf)
	record.HeaderSize = headerSize

	// Read column types
	numCols := 0
	for n < int(headerSize) {
		colType, n1 := ReadVarInt(buf[n:])
		record.ColumnTypes = append(record.ColumnTypes, colType)
		n += n1
		numCols++
	}

	// Read keys
	for i := range numCols {
		colType := record.ColumnTypes[i]
		switch {
		case colType == 0:
			record.Keys = append(record.Keys, []byte{0})
			continue
		case colType == 1:
			// Read 8-bit 2's complement integer
			record.Keys = append(record.Keys, buf[n:n+1])
			n++
		case colType == 2:
			// Read 16-bit big-endian integer
			record.Keys = append(record.Keys, buf[n:n+2])
			n += 2
		case colType == 3:
			// Read 24-bit big-endian integer
			record.Keys = append(record.Keys, buf[n:n+3])
			n += 3
		case colType == 4:
			// Read 32-bit big-endian integer
			record.Keys = append(record.Keys, buf[n:n+4])
			n += 4
		case colType == 5:
			// Read 48-bit big-endian integer
			record.Keys = append(record.Keys, buf[n:n+6])
			n += 6
		case colType == 6:
			// Read 64-bit big-endian integer
			record.Keys = append(record.Keys, buf[n:n+8])
			n += 8
		case colType == 7:
			// Read 64-bit IEEE floating point
			record.Keys = append(record.Keys, buf[n:n+8])
			n += 8
		case colType == 8:
			// Value is the integer 0
			record.Keys = append(record.Keys, []byte{0})
		case colType == 9:
			// Value is the integer 1
			record.Keys = append(record.Keys, []byte{1})
		case colType == 10, colType == 11:
			continue
		case colType >= 12 && colType%2 == 0:
			keyLen := (record.ColumnTypes[i] - 12) / 2
			endIdx := min(int(keyLen)+n, len(buf))
			record.Keys = append(record.Keys, buf[n:endIdx])
			n += int(keyLen)
		case colType >= 13 && colType%2 == 1:
			keyLen := (record.ColumnTypes[i] - 13) / 2
			endIdx := min(int(keyLen)+n, len(buf))
			record.Keys = append(record.Keys, buf[n:endIdx])
			n += int(keyLen)
		}
	}

	return record
}

func (rootPage *Page) GetTableColumnNames(name string) ([]string, error) {
	if rootPage.Header.Type != LeafTableBTreePage {
		return nil, errors.New("page is not a root page")
	}

	tableNames, err := rootPage.GetTableNames()
	if err != nil {
		return nil, err
	}

	var rowNum int
	for i, tableName := range tableNames {
		if tableName == name {
			rowNum = i
			break
		}
	}

	return rootPage.Tables[rowNum].ColNames, nil
}

func (page *Page) GetTablebyName(name string) (*Page, error) {
	if page.Header.Type != LeafTableBTreePage {
		return nil, errors.New("not a table page")
	}

	newPage, err := page.GetPageByName(name)
	if err != nil {
		return nil, err
	}

	return newPage, nil
}

func (page *Page) GetTableColumn(colNum int) []string {
	if page.Header.Type == LeafTableBTreePage || page.Header.Type == LeafIndexBTreePage {
		result := make([]string, 0)
		for _, cell := range page.Cells {
			if colNum == -1 {
				result = append(result, strconv.Itoa(int(cell.RowID)))
			} else {
				result = append(result, string(cell.Record.Keys[colNum]))
			}
		}
		return result
	}

	result := make([]string, 0)
	for _, page := range page.Pages {
		result = append(result, page.GetTableColumn(colNum)...)
	}

	return result

}

func (page *Page) GetTableColumns(colNames []string) [][]string {
	if page.Header.Type == LeafTableBTreePage || page.Header.Type == LeafIndexBTreePage {
		result := make([][]string, 0)
		for _, cell := range page.Cells {
			row := []string{}
			for i, colName := range colNames {
				if strings.Contains(colName, "id") {
					row = append(row, strconv.Itoa(int(cell.RowID)))
					continue
				}
				row = append(row, string(cell.Record.Keys[i]))
			}
			result = append(result, row)
		}
		return result
	}

	result := make([][]string, 0)
	for _, page := range page.Pages {
		result = append(result, page.GetTableColumns(colNames)...)
	}
	return result
}

func (page *Page) GetTableRowCount() int {
	if page.Header.Type == LeafTableBTreePage || page.Header.Type == LeafIndexBTreePage {
		return int(page.Header.CellCount)
	}
	count := 0
	for _, page := range page.Pages {
		count += page.GetTableRowCount()
	}
	return count
}
