package main

import (
	"bytes"
	"encoding/binary"
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
	CellPtrs []int16
	Cells    []*Cell
	Pages    []*Page
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
	// PayloadSizeOffset int32  // Used for: IndexLeafCell, IndexLeafCell, IndexInteriorCell
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

func NewPage(header *Header) *Page {
	page := &Page{
		Header: header,
	}

	return page
}

func ReadDatabaseFile(databaseFilePath string) *Page {
	// Open database file
	databaseFile, err := os.Open(databaseFilePath)
	if err != nil {
		log.Fatal(err)
	}
	defer databaseFile.Close()

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

func TraverseBTree(databaseFile *os.File, pageSize uint16, pageNumber uint32) *Page {
	offset := int64(pageNumber)*int64(pageSize) + 100

	header := ReadHeader(offset, databaseFile)
	page := NewPage(header)
	page.Size = pageSize

	err := ReadCellPtrs(offset, page, databaseFile)
	if err != nil {
		log.Fatal(err)
	}

	if page.Header.Type == LeafTableBTreePage || page.Header.Type == LeafIndexBTreePage {
		return page
	}

	for _, cell := range page.Cells {
		pageNumber := binary.BigEndian.Uint32(cell.Payload)
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

func ReadCellPtrs(offset int64, page *Page, file *os.File) error {
	// Read cell pointers
	page.CellPtrs = make([]int16, page.Header.CellCount)
	var arrayStart int
	if page.Header.Type == InteriorTableBTreePage || page.Header.Type == InteriorIndexBTreePage {
		arrayStart = int(offset) + 12
	} else {
		arrayStart = int(offset) + 8
	}

	file.Seek(int64(arrayStart), 0)
	for i := 0; i < int(page.Header.CellCount); i++ {
		var cellPtr int16
		err := binary.Read(file, binary.BigEndian, &cellPtr)
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
		err = ReadTableLeafCell(page, file)
	case LeafIndexBTreePage:
		err = ReadIndexLeafCell(page, file)
	case InteriorIndexBTreePage:
		err = ReadIndexInteriorCell(page, file)
	case InteriorTableBTreePage:
		err = ReadTableInteriorCell(page, file)
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
func ReadTableLeafCell(page *Page, file *os.File) error {
	for i := 0; i < int(page.Header.CellCount); i++ {
		cellPtr := page.CellPtrs[i]
		buf := make([]byte, int(page.Size)-int(cellPtr))
		file.ReadAt(buf, int64(cellPtr))

		cell := &Cell{}

		// Read payload size
		payloadSize, n := binary.Uvarint(buf)
		cell.PayloadSize = payloadSize

		// Read row ID
		rowID, n1 := binary.Uvarint(buf[n:])
		cell.RowID = rowID

		// Read payload
		cell.Payload = make([]byte, cell.PayloadSize)
		file.ReadAt(cell.Payload, int64(cellPtr)+int64(n+n1))

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
func ReadIndexLeafCell(page *Page, file *os.File) error {
	for i := 0; i < int(page.Header.CellCount); i++ {
		cellPtr := page.CellPtrs[i]
		buf := make([]byte, int(page.Size)-int(cellPtr))
		file.ReadAt(buf, int64(cellPtr))

		cell := &Cell{}

		// Read payload size
		payloadSize, n := binary.Uvarint(buf)
		cell.PayloadSize = payloadSize

		// Read payload
		cell.Payload = make([]byte, cell.PayloadSize)
		file.ReadAt(cell.Payload, int64(cellPtr)+int64(n))

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
func ReadIndexInteriorCell(page *Page, file *os.File) error {
	for i := 0; i < int(page.Header.CellCount); i++ {
		cellPtr := page.CellPtrs[i]
		buf := make([]byte, int(page.Size)-int(cellPtr))
		file.ReadAt(buf, int64(cellPtr))

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
		file.ReadAt(cell.Payload, int64(cellPtr)+int64(n+4))

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
func ReadTableInteriorCell(page *Page, file *os.File) error {
	for i := 0; i < int(page.Header.CellCount); i++ {
		cellPtr := page.CellPtrs[i]
		buf := make([]byte, int(page.Size)-int(cellPtr))
		file.ReadAt(buf, int64(cellPtr))

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

func GetTableCount(rootPage *Page) int {
	if rootPage.Header.Type == LeafTableBTreePage {
		return len(rootPage.Cells)
	}

	var count int
	for _, childPage := range rootPage.Pages {
		count += GetTableCount(childPage)
	}

	return count
}

func GetTableNames(rootPage *Page) []string {
	if rootPage.Header.Type == LeafTableBTreePage {
		var tableNames []string
		for _, cell := range rootPage.Cells {
			payload := string(cell.Payload)
			idx := strings.Index(payload, "CREATE TABLE") + 13
			if idx != -1 {
				payload = strings.TrimSpace(payload[idx:strings.Index(payload, "(")])
				if strings.Contains(payload, "sqlite_") {
					continue
				}
				tableNames = append(tableNames, payload)
			}
		}

		return tableNames
	}

	var tableNames []string
	for _, childPage := range rootPage.Pages {
		tableNames = append(tableNames, GetTableNames(childPage)...)
	}

	return tableNames
}
