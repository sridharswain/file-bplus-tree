package btree

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"strconv"
	"sync"
)

const (
	MetadataSize   = 1 * 1024
	IndexBlockSize = 4 * 1024
	PageBlockSize  = 16 * 1024
)

var BUFFER_POOL map[int]*sync.Pool = createBufferPool()

func createBufferPool() map[int]*sync.Pool {
	pool := map[int]*sync.Pool{}

	pool[MetadataSize] = &sync.Pool{New: func() any { return (make([]byte, MetadataSize)) }}
	pool[IndexBlockSize] = &sync.Pool{New: func() any { return make([]byte, IndexBlockSize) }}
	pool[PageBlockSize] = &sync.Pool{New: func() any { return make([]byte, PageBlockSize) }}

	return pool
}

type PageBlockType struct {
	PageType string
}

type PageBlock[TKey, TValue any] interface {
	*DataPage[TKey, TValue] | *IndexPage[TKey, TValue] | *BTree[TKey, TValue]
}

func SaveAt[TKey, TValue any, TPageBlock PageBlock[TKey, TValue]](tree *BTree[TKey, TValue], page TPageBlock, file *os.File, offset int, length int) {

	binBytes := new(bytes.Buffer)
	enc := gob.NewEncoder(binBytes)
	err := enc.Encode(page)

	if err != nil {
		panic(err)
	}

	var writeBytes []byte = formatBytesToWrite(binBytes, length)

	if _, err = file.WriteAt(writeBytes, int64(offset)); err != nil {
		panic(err)
	}
}

func SaveDataPage[TKey, TValue any](tree *BTree[TKey, TValue], page *DataPage[TKey, TValue], file *os.File, offset int) {
	page.Offset = offset
	SaveAt(tree, page, file, offset, PageBlockSize)
}

func SaveIndexPage[TKey, TValue any](tree *BTree[TKey, TValue], page *IndexPage[TKey, TValue], file *os.File, offset int) {
	page.Offset = offset
	SaveAt(tree, page, file, offset, IndexBlockSize)
}

func SaveMetadata[TKey, TValue any](tree *BTree[TKey, TValue], file *os.File) {
	SaveAt(tree, tree, file, 0, MetadataSize)
}

func ReadAt[TKey, TValue any, TPageBlock PageBlock[TKey, TValue]](page TPageBlock, file *os.File, offset int, length int) TPageBlock {

	var datatToUnmarshal []byte

	var buffer []byte = BUFFER_POOL[length].Get().([]byte)
	defer BUFFER_POOL[length].Put(buffer)
	_, err := file.ReadAt(buffer, int64(offset))

	if err != nil {
		panic(err)
	}

	lengthBytes := buffer[:bytes.IndexRune(buffer, ':')]
	dataLength, _ := strconv.Atoi(string(lengthBytes))

	datatToUnmarshal = buffer[len(lengthBytes)+1 : len(lengthBytes)+1+dataLength]

	dec := gob.NewDecoder(bytes.NewBuffer(datatToUnmarshal))

	err = dec.Decode(page)

	if err != nil {
		panic(err)
	}

	return page
}

func ReadDataPage[TKey, TValue any](tree *BTree[TKey, TValue], file *os.File, offset int) *DataPage[TKey, TValue] {
	var page DataPage[TKey, TValue]
	page = *ReadAt[TKey, TValue](&page, file, offset, PageBlockSize)
	page.tree = tree
	return &page
}

func ReadIndexPage[TKey, TValue any](tree *BTree[TKey, TValue], file *os.File, offset int) *IndexPage[TKey, TValue] {
	var page IndexPage[TKey, TValue]
	page = *ReadAt[TKey, TValue](&page, file, offset, IndexBlockSize)
	page.tree = tree
	return &page
}

func ReadMetadata[TKey, TValue any](file *os.File) *BTree[TKey, TValue] {
	var page BTree[TKey, TValue]
	return ReadAt[TKey, TValue](&page, file, 0, MetadataSize)
}

func formatBytesToWrite(binBuffer *bytes.Buffer, length int) []byte {
	var writeBytes []byte = make([]byte, length)
	dataBytes := binBuffer.Bytes()
	metaBytes := []byte(fmt.Sprintf("%s:", strconv.Itoa(len(dataBytes))))

	copy(writeBytes[:len(metaBytes)], metaBytes)
	copy(writeBytes[len(metaBytes):len(metaBytes)+len(dataBytes)], dataBytes)

	return writeBytes
}
