package main

import (
	"bytes"
	"cmp"
	"encoding/binary"
	// "encoding/json"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto"
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

func getFromCache(offset int) ([]byte, bool) {
	data, exists := PAGE_CACHE.Get(strconv.Itoa(offset))
	if !exists {
		return nil, exists
	}
	return data.([]byte), exists
}

func setInCache(offset int, value []byte) {
	PAGE_CACHE.SetWithTTL(strconv.Itoa(offset), value, int64(len(value)), time.Hour)
}

func deleteInCache(offset int) {
	PAGE_CACHE.Del(strconv.Itoa(offset))
}

type PageBlockType struct {
	PageType string
}

type PageBlock[TKey cmp.Ordered, TValue any] interface {
	*DataPage[TKey, TValue] | *IndexPage[TKey, TValue] | *BTree[TKey, TValue] | *PageBlockType
}

// func SaveAt[TKey cmp.Ordered, TValue any, TTPage PageBlock[TKey, TValue]](indexName string, page TTPage, offset int, length int) {

// 	jsonBytes, err := json.Marshal(page)

// 	if err != nil {
// 		panic(err)
// 	}

// 	f, err := os.OpenFile(indexName, os.O_WRONLY|os.O_CREATE, os.ModePerm)
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer f.Close()

// 	var writeBytes []byte = make([]byte, length)
// 	copy(writeBytes[:len(jsonBytes)], jsonBytes)

// 	if _, err = f.WriteAt(writeBytes, int64(offset)); err != nil {
// 		panic(err)
// 	}
// 	deleteInCache(offset)
// }

func SaveAt[TKey cmp.Ordered, TValue any, TTPage PageBlock[TKey, TValue]](indexName string, page TTPage, offset int, length int) {

	binBytes := new(bytes.Buffer)
	err := binary.Write(binBytes, binary.BigEndian, page)

	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile(indexName, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var writeBytes []byte = make([]byte, length)
	bytesToWrite := binBytes.Bytes()
	copy(writeBytes[:len(bytesToWrite)], bytesToWrite)

	if _, err = f.WriteAt(writeBytes, int64(offset)); err != nil {
		panic(err)
	}
	deleteInCache(offset)
}

func SaveDataPage[TKey cmp.Ordered, TValue any](indexName string, page *DataPage[TKey, TValue], offset int) {
	SaveAt[TKey, TValue](indexName, page, offset, PAGE_BLOCK_SIZE)
	page.Offset = offset
}

func SaveIndexPage[TKey cmp.Ordered, TValue any](indexName string, page *IndexPage[TKey, TValue], offset int) {
	SaveAt[TKey, TValue](indexName, page, offset, INDEX_BLOCK_SIZE)
	page.Offset = offset
}

func SaveMetadata[TKey cmp.Ordered, TValue any](indexName string, page *BTree[TKey, TValue]) {
	SaveAt[TKey, TValue](indexName, page, 0, METADATA_SIZE)
}

// func ReadAt[TKey cmp.Ordered, TValue any, TTPage PageBlock[TKey, TValue]](indexName string, page TTPage, offset int, length int) []byte {
// 	var datatToUnmarshal []byte

// 	datatToUnmarshal, exists := getFromCache(offset)
// 	if !exists {
// 		file, err := os.OpenFile(indexName, os.O_RDONLY, os.ModePerm)

// 		if err != nil {
// 			panic(err)
// 		}
// 		defer file.Close()

// 		var buffer []byte = BUFFER_POOL[length].Get().([]byte)
// 		defer BUFFER_POOL[length].Put(buffer)
// 		_, err = file.ReadAt(buffer, int64(offset))

// 		if err != nil {
// 			panic(err)
// 		}
// 		jsonBytes := (buffer)[:bytes.IndexByte(buffer, 0)]
// 		datatToUnmarshal = make([]byte, len(jsonBytes))
// 		copy(datatToUnmarshal, jsonBytes)

// 		setInCache(offset, datatToUnmarshal)
// 	}

// 	err := json.Unmarshal(datatToUnmarshal, page)

// 	if err != nil {
// 		panic(err)
// 	}

// 	return datatToUnmarshal
// }

func ReadAt[TKey cmp.Ordered, TValue any, TTPage PageBlock[TKey, TValue]](indexName string, page TTPage, offset int, length int) []byte {
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

	binBytes := buffer[:bytes.IndexByte(buffer, 0)]
	datatToUnmarshal = make([]byte, len(binBytes))
	copy(datatToUnmarshal, binBytes)

	// setInCache(offset, datatToUnmarshal)
	err = binary.Read(bytes.NewBuffer(datatToUnmarshal), binary.BigEndian, page)

	// err = json.Unmarshal(datatToUnmarshal, page)

	if err != nil {
		panic(err)
	}

	return datatToUnmarshal
}

func ReadDataPage[TKey cmp.Ordered, TValue any](tree *BTree[TKey, TValue], page *DataPage[TKey, TValue], offset int) {
	ReadAt[TKey, TValue](tree.IndexName, page, offset, PAGE_BLOCK_SIZE)
	page.tree = tree
}

func ReadIndexPage[TKey cmp.Ordered, TValue any](tree *BTree[TKey, TValue], page *IndexPage[TKey, TValue], offset int) {
	ReadAt[TKey, TValue](tree.IndexName, page, offset, INDEX_BLOCK_SIZE)
	page.tree = tree
}

func ReadMetadata[TKey cmp.Ordered, TValue any](indexName string, page *BTree[TKey, TValue]) {
	ReadAt[TKey, TValue](indexName, page, 0, METADATA_SIZE)
}

func indexFileExists(indexName string) bool {
	if _, err := os.Stat(indexName); err == nil {
		return true
	}

	return false
}
