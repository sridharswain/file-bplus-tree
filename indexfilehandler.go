package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"os"
)

const (
	DATA_PAGE  = "DATA_PAGE"
	INDEX_PAGE = "INDEX_PAGE"
)

const (
	METADATA_SIZE    = 1 * 1024
	INDEX_BLOCK_SIZE = 1 * 1024
	PAGE_BLOCK_SIZE  = 1 * 1024
)

type PageBlockType struct {
	PageType string
}

type PageBlock[TKey cmp.Ordered, TValue any] interface {
	*DataPage[TKey, TValue] | *IndexPage[TKey, TValue] | *BTree[TKey, TValue] | *PageBlockType
}

func SaveAt[TKey cmp.Ordered, TValue any, TTPage PageBlock[TKey, TValue]](indexName string, page TTPage, offset int, length int) {

	jsonBytes, err := json.Marshal(page)

	if err != nil {
		panic(err)
	}

	f, err := os.OpenFile(indexName, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	var writeBytes []byte = make([]byte, length)
	copy(writeBytes[:len(jsonBytes)], jsonBytes)

	if _, err = f.WriteAt(writeBytes, int64(offset)); err != nil {
		panic(err)
	}
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

func ReadAt[TKey cmp.Ordered, TValue any, TTPage PageBlock[TKey, TValue]](indexName string, page TTPage, offset int, length int) []byte {
	file, err := os.OpenFile(indexName, os.O_RDONLY, os.ModePerm)

	if err != nil {
		panic(err)
	}
	defer file.Close()

	var buffer []byte = make([]byte, length)
	_, err = file.ReadAt(buffer, int64(offset))

	if err != nil {
		panic(err)
	}
	jsonBytes := buffer[:bytes.IndexByte(buffer, 0)]
	var dataToRead []byte = make([]byte, len(jsonBytes))
	copy(dataToRead, jsonBytes)

	err = json.Unmarshal(dataToRead, page)

	if err != nil {
		panic(err)
	}

	return jsonBytes
}

func ReadDataPage[TKey cmp.Ordered, TValue any](tree *BTree[TKey, TValue], page *DataPage[TKey, TValue], offset int) {
	ReadAt[TKey, TValue](tree.indexName, page, offset, PAGE_BLOCK_SIZE)
	page.tree = tree
}

func ReadIndexPage[TKey cmp.Ordered, TValue any](tree *BTree[TKey, TValue], page *IndexPage[TKey, TValue], offset int) {
	ReadAt[TKey, TValue](tree.indexName, page, offset, INDEX_BLOCK_SIZE)
	page.tree = tree
}

func ReadMetadata[TKey cmp.Ordered, TValue any](indexName string, page *BTree[TKey, TValue]) {
	ReadAt[TKey, TValue](indexName, page, 0, METADATA_SIZE)
}
