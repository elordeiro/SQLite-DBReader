package main

import (
	"bytes"
	"encoding/binary"
	"slices"
	"strconv"
	"strings"
)

// Constants ------------------------------------------------------------------

const (
	LCPLen       = 4 // Left child pointer length
	MaxVarIntLen = 9 // Max length of an unsigned variable length integer
)

const (
	IndexPageKeyIdx   = 0
	IndexPageRowIdIdx = 1
)

// ----------------------------------------------------------------------------

// Custom Types----------------------------------------------------------------
type Page struct {
	Header        *Header
	CellPtrs      []int
	Cells         []*Cell
	FilteredCells []*Cell
	Pages         []*Page
}

type Header struct {
	Type             uint8
	CellCount        int
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
	HeaderSize  int       // Part of Header
	ColumnTypes []uint64  // Part of Header
	Keys        [][]uint8 // Part of Body
	RowID       int64     // Part of Body -- Not present in table leaf cells
}

// ----------------------------------------------------------------------------

// Parser functions -----------------------------------------------------------
func ParseHeader(buf []byte) *Header {
	// Read page type
	pageType := buf[0]

	// Read cell count
	var cc uint16
	binary.Read(bytes.NewReader(buf[3:5]), binary.BigEndian, &cc)
	cellCount := int(cc)

	// Read right most pointer
	var rightMostPointer uint32
	if pageType == InteriorTablePage || pageType == InteriorIndexPage {
		binary.Read(bytes.NewReader(buf[8:12]), binary.BigEndian, &rightMostPointer)
	}

	return &Header{
		pageType,
		cellCount,
		rightMostPointer,
	}
}

func ParseCellPtrs(buf []byte, header *Header) []int {
	// Read cell pointers
	cellPtrs := make([]int, header.CellCount)
	offset := int64(8)
	if header.Type == InteriorTablePage || header.Type == InteriorIndexPage {
		offset += 4
	}

	for i := 0; i < header.CellCount; i++ {
		var cellPtr uint16
		binary.Read(bytes.NewReader(buf[offset:]), binary.BigEndian, &cellPtr)
		cellPtrs[i] = int(cellPtr)
		offset += 2
	}

	return cellPtrs
}

/*
Table B-Tree Interior Cell (header 0x05):

	A 4-byte big-endian page number which is the left child pointer.
	A varint which is the integer key
*/
func (page *Page) ParseInteriorTableCells(pageBuf []byte) {
	page.Cells = make([]*Cell, 0)
	for i := 0; i < page.Header.CellCount; i++ {
		off := page.CellPtrs[i]

		cell := &Cell{}

		// Read left child pointer
		binary.Read(bytes.NewReader(pageBuf[off:off+LCPLen]), binary.BigEndian, &cell.LeftChildPointer)
		off += LCPLen

		// Read row ID
		cell.RowID, _ = parseVarInt(pageBuf[off:])

		// Append cell to page cells
		page.Cells = append(page.Cells, cell)
	}
}

// This function works simlilarly to ParseInteriorTableCells(), but also takes a *[]uint64 as a filter
func (page *Page) ParseInteriorTableCellsFiltered(pageBuf []byte, rowIDs *[]uint64) {
	page.Cells = make([]*Cell, 0)
	for i := 0; i < page.Header.CellCount; i++ {
		off := page.CellPtrs[i]

		cell := &Cell{}

		// Read left child pointer
		binary.Read(bytes.NewReader(pageBuf[off:off+LCPLen]), binary.BigEndian, &cell.LeftChildPointer)
		off += LCPLen

		// Read row ID
		cell.RowID, _ = parseVarInt(pageBuf[off:])

		// Append cell to page cells
		if slices.ContainsFunc(*rowIDs, func(id uint64) bool {
			return id <= cell.RowID
		}) {
			page.Cells = append(page.Cells, cell)
		}
	}

	page.Cells = append(page.Cells, &Cell{
		LeftChildPointer: page.Header.RightMostPointer,
	})
}

/*
Table B-Tree Leaf Cell (header 0x0d):

	A varint which is the total number of bytes of payload, including any overflow
	A varint which is the integer key, a.k.a. "rowid"
	The initial portion of the payload that does not spill to overflow pages.
	A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
*/
func (page *Page) ParseLeafTableCells(pageBuf []byte) {
	page.Cells = make([]*Cell, 0)
	for i := 0; i < page.Header.CellCount; i++ {
		off := page.CellPtrs[i]

		cell := &Cell{}

		// Read payload size
		payloadSize, n := parseVarInt(pageBuf[off : off+MaxVarIntLen])
		cell.PayloadSize = payloadSize
		off += n

		// Read row ID
		rowID, n := parseVarInt(pageBuf[off : off+MaxVarIntLen])
		cell.RowID = rowID
		off += n

		// Read Record
		cell.Record = ReadRecord(pageBuf[off : off+int(payloadSize)])

		// Append cell to page cells
		page.Cells = append(page.Cells, cell)
	}
}

// This function works simlilarly to ParseLeafTableCells(), but also takes a *[]uint64 as a filter
func (page *Page) ParseLeafTableCellsFiltered(pageBuf []byte, rowIDs *[]uint64) {
	page.Cells = make([]*Cell, 0)
	for i := 0; i < page.Header.CellCount; i++ {
		off := page.CellPtrs[i]

		cell := &Cell{}

		// Read payload size
		payloadSize, n := parseVarInt(pageBuf[off : off+MaxVarIntLen])
		cell.PayloadSize = payloadSize
		off += n

		// Read row ID
		rowID, n := parseVarInt(pageBuf[off : off+MaxVarIntLen])
		cell.RowID = rowID
		off += n

		// Read Record
		cell.Record = ReadRecord(pageBuf[off : off+int(payloadSize)])

		// Append cell to page cells
		if slices.Contains(*rowIDs, cell.RowID) {
			page.Cells = append(page.Cells, cell)
			*rowIDs = (*rowIDs)[1:]
		}
	}
}

/*
Index B-Tree Interior Cell (header 0x02):

	A 4-byte big-endian page number which is the left child pointer.
	A varint which is the total number of bytes of key payload, including any overflow
	The initial portion of the payload that does not spill to overflow pages.
	A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
*/
func (page *Page) ParseInteriorIndexCells(pageBuf []byte, key string) {
	page.Cells = make([]*Cell, 0)
	for i := 0; i < page.Header.CellCount; i++ {
		off := page.CellPtrs[i]

		cell := &Cell{}

		// Read left child pointer
		binary.Read(bytes.NewReader(pageBuf[off:off+LCPLen]), binary.BigEndian, &cell.LeftChildPointer)
		off += LCPLen

		// Read payload size
		payloadSize, n := parseVarInt(pageBuf[off:])
		cell.PayloadSize = payloadSize
		off += n

		// Read payload
		cell.Record = ReadRecord(pageBuf[off : off+int(payloadSize)])

		// Append cell to page cells
		page.Cells = append(page.Cells, cell)
		if string(cell.Record.Keys[IndexPageKeyIdx]) == key {
			page.FilteredCells = append(page.FilteredCells, cell)
		}
	}
}

/*
Index B-Tree Leaf Cell (header 0x0a):

	A varint which is the total number of bytes of key payload, including any overflow
	The initial portion of the payload that does not spill to overflow pages.
	A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
*/
func (page *Page) ParseLeafIndexCells(pageBuf []byte, key string) {
	page.Cells = make([]*Cell, 0)
	for i := 0; i < page.Header.CellCount; i++ {
		off := page.CellPtrs[i]

		cell := &Cell{}

		// Read payload size
		payloadSize, n := parseVarInt(pageBuf[off:])
		cell.PayloadSize = payloadSize
		off += n

		// Read payload
		cell.Record = ReadRecord(pageBuf[off : off+int(payloadSize)])

		// Append cell to page cells
		page.Cells = append(page.Cells, cell)
		if string(cell.Record.Keys[IndexPageKeyIdx]) == key {
			page.FilteredCells = append(page.FilteredCells, cell)
		}
	}
}

func ReadRecord(buf []byte) *Record {
	record := &Record{
		Keys: make([][]uint8, 0),
	}

	// Read header size
	headerSize, n := parseVarInt(buf)
	record.HeaderSize = int(headerSize)

	// Read column types
	numCols := 0
	for n < record.HeaderSize {
		colType, n1 := parseVarInt(buf[n:])
		record.ColumnTypes = append(record.ColumnTypes, colType)
		n += n1
		numCols++
	}

	off := uint64(n)

	// Read keys
	for i := range numCols {
		colType := record.ColumnTypes[i]
		switch {
		case colType == 0:
			record.Keys = append(record.Keys, []byte{0})
			continue
		case colType == 1:
			// Read 8-bit 2's complement integer
			record.Keys = append(record.Keys, buf[off:off+1])
			off++
		case colType == 2:
			// Read 16-bit big-endian integer
			record.Keys = append(record.Keys, buf[off:off+2])
			off += 2
		case colType == 3:
			// Read 24-bit big-endian integer
			record.Keys = append(record.Keys, buf[off:off+3])
			off += 3
		case colType == 4:
			// Read 32-bit big-endian integer
			record.Keys = append(record.Keys, buf[off:off+4])
			off += 4
		case colType == 5:
			// Read 48-bit big-endian integer
			record.Keys = append(record.Keys, buf[off:off+6])
			off += 6
		case colType == 6:
			// Read 64-bit big-endian integer
			record.Keys = append(record.Keys, buf[off:off+8])
			off += 8
		case colType == 7:
			// Read 64-bit IEEE floating point
			record.Keys = append(record.Keys, buf[off:off+8])
			off += 8
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
			record.Keys = append(record.Keys, buf[off:off+keyLen])
			off += keyLen
		case colType >= 13 && colType%2 == 1:
			keyLen := (record.ColumnTypes[i] - 13) / 2
			record.Keys = append(record.Keys, buf[off:off+keyLen])
			off += keyLen
		}
	}

	return record
}

// ----------------------------------------------------------------------------

// Cell parser helpers --------------------------------------------------------
// Big-endian
func parseVarInt(buf []byte) (uint64, int) {
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

func bytesToInt(bytes []byte) uint64 {
	var result uint64
	for _, b := range bytes {
		result = (result << 8) | uint64(b)
	}
	return result
}

func parseTableType(typ []uint8) int {
	switch string(typ) {
	case "table":
		return TableTypeTable
	case "index":
		return TableTypeIndex
	case "view":
		return TableTypeView
	case "trigger":
		return TableTypeTrigger
	default:
		return 0
	}
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

// ----------------------------------------------------------------------------

// Getters --------------------------------------------------------------------
func (page *Page) GetAllRows(colNames []string) [][]string {
	if page.Header.Type == LeafTablePage || page.Header.Type == LeafIndexPage {
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
		result = append(result, page.GetAllRows(colNames)...)
	}
	return result
}

func (page *Page) GetFilteredRowIDs() []uint64 {
	result := make([]uint64, 0)
	for _, cell := range page.FilteredCells {
		rowId := bytesToInt(cell.Record.Keys[IndexPageRowIdIdx])
		result = append(result, rowId)
	}

	for _, p := range page.Pages {
		result = append(result, p.GetFilteredRowIDs()...)
	}
	return result
}

// ----------------------------------------------------------------------------
