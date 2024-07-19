package main

import (
	"bytes"
	"cmp"
	"encoding/gob"
	"fmt"

	"github.com/dgraph-io/ristretto"
	"os"
	"strconv"
	"sync"
)

const (
	DATA_PAGE  = "DATA_PAGE"
	INDEX_PAGE = "INDEX_PAGE"
)

const (
	METADATA_SIZE    = 1 * 1024
	INDEX_BLOCK_SIZE = 4 * 1024
	PAGE_BLOCK_SIZE  = 8 * 1024
)

var BUFFER_POOL map[int]*sync.Pool = createBufferPool()
var PAGE_CACHE *ristretto.Cache = createCache()

func createCache() *ristretto.Cache {
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,     // number of keys to track frequency of (10M).
		MaxCost:     1 << 30, // maximum cost of cache (1GB).
		BufferItems: 64,      // number of keys per Get buffer.
	})
	if err != nil {
		panic(err)
	}
	return cache
}

func createBufferPool() map[int]*sync.Pool {
	pool := map[int]*sync.Pool{}

	pool[METADATA_SIZE] = &sync.Pool{New: func() any { return (make([]byte, METADATA_SIZE)) }}
	pool[INDEX_BLOCK_SIZE] = &sync.Pool{New: func() any { return make([]byte, INDEX_BLOCK_SIZE) }}
	pool[PAGE_BLOCK_SIZE] = &sync.Pool{New: func() any { return make([]byte, PAGE_BLOCK_SIZE) }}

	return pool
}

func getFromCache[TKey cmp.Ordered, TValue any, TPageBlock PageBlock[TKey, TValue]](offset int) (TPageBlock, bool) {
	data, exists := PAGE_CACHE.Get(offset)
	if !exists {
		return nil, exists
	}
	return data.(TPageBlock), exists
}

func setInCache[TKey cmp.Ordered, TValue any, TPageBlock PageBlock[TKey, TValue]](offset int, value TPageBlock) {
	// PAGE_CACHE.SetWithTTL(strconv.Itoa(offset), value, int64(len(value)), time.Hour)
	PAGE_CACHE.Set(offset, value, 1)
}

func deleteInCache(offset int) {
	PAGE_CACHE.Del(offset)
}

type PageBlockType struct {
	PageType string
}

type PageBlock[TKey cmp.Ordered, TValue any] interface {
	*DataPage[TKey, TValue] | *IndexPage[TKey, TValue] | *BTree[TKey, TValue]
}

func SaveAt[TKey cmp.Ordered, TValue any, TPageBlock PageBlock[TKey, TValue]](tree *BTree[TKey, TValue], page TPageBlock, offset int, length int) {

	binBytes := new(bytes.Buffer)
	enc := gob.NewEncoder(binBytes)
	err := enc.Encode(page)

	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile(tree.IndexName, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var writeBytes []byte = formatBytesToWrite(binBytes, length)

	if _, err = f.WriteAt(writeBytes, int64(offset)); err != nil {
		panic(err)
	}
	deleteInCache(offset)
}

func SaveDataPage[TKey cmp.Ordered, TValue any](tree *BTree[TKey, TValue], page *DataPage[TKey, TValue], offset int) {
	page.Offset = offset
	SaveAt(tree, page, offset, PAGE_BLOCK_SIZE)
}

func SaveIndexPage[TKey cmp.Ordered, TValue any](tree *BTree[TKey, TValue], page *IndexPage[TKey, TValue], offset int) {
	page.Offset = offset
	SaveAt(tree, page, offset, INDEX_BLOCK_SIZE)
}

func SaveMetadata[TKey cmp.Ordered, TValue any](tree *BTree[TKey, TValue]) {
	SaveAt(tree, tree, 0, METADATA_SIZE)
}

func ReadAt[TKey cmp.Ordered, TValue any, TPageBlock PageBlock[TKey, TValue]](indexName string, page TPageBlock, offset int, length int) TPageBlock {

	cached, found := getFromCache[TKey, TValue, TPageBlock](offset)

	if found {
		return cached
	} else {
		var datatToUnmarshal []byte
		file, err := os.OpenFile(indexName, os.O_RDONLY, os.ModePerm)

		if err != nil {
			panic(err)
		}
		defer file.Close()

		var buffer []byte = BUFFER_POOL[length].Get().([]byte)
		defer BUFFER_POOL[length].Put(buffer)
		_, err = file.ReadAt(buffer, int64(offset))

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

		setInCache[TKey, TValue](offset, page)

		return page
	}
}

func ReadDataPage[TKey cmp.Ordered, TValue any](tree *BTree[TKey, TValue], offset int) *DataPage[TKey, TValue] {
	var page DataPage[TKey, TValue]
	page = *ReadAt[TKey, TValue](tree.IndexName, &page, offset, PAGE_BLOCK_SIZE)
	page.tree = tree
	return &page
}

func ReadIndexPage[TKey cmp.Ordered, TValue any](tree *BTree[TKey, TValue], offset int) *IndexPage[TKey, TValue] {
	var page IndexPage[TKey, TValue]
	page = *ReadAt[TKey, TValue](tree.IndexName, &page, offset, INDEX_BLOCK_SIZE)
	page.tree = tree
	return &page
}

func ReadMetadata[TKey cmp.Ordered, TValue any](indexName string) *BTree[TKey, TValue] {
	var page BTree[TKey, TValue]
	return ReadAt[TKey, TValue](indexName, &page, 0, METADATA_SIZE)
}

func indexFileExists(indexName string) bool {
	if _, err := os.Stat(indexName); err == nil {
		return true
	}

	return false
}

func formatBytesToWrite(binBuffer *bytes.Buffer, length int) []byte {
	var writeBytes []byte = make([]byte, length)
	dataBytes := binBuffer.Bytes()
	metaBytes := []byte(fmt.Sprintf("%s:", strconv.Itoa(len(dataBytes))))

	copy(writeBytes[:len(metaBytes)], metaBytes)
	copy(writeBytes[len(metaBytes):len(metaBytes)+len(dataBytes)], dataBytes)

	return writeBytes
}
