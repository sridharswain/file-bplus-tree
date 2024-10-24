package btree

import "os"

type Enumerator[TKey, V any] struct {
	originalKeyFound bool
	i                int
	dataPage         *DataPage[TKey, V]
	tree             *BTree[TKey, V]
}

func (enumerator *Enumerator[TKey, V]) Next(file *os.File) (*TKey, *V) {
	if !enumerator.HasNext() {
		return nil, nil
	}

	if enumerator.i < enumerator.dataPage.Count-1 {
		enumerator.i++
		key := &enumerator.dataPage.Container[enumerator.i].Key
		value := &enumerator.dataPage.Container[enumerator.i].Value
		return key, value
	} else {
		enumerator.dataPage = ReadDataPage(enumerator.tree, file, enumerator.dataPage.Next)
		enumerator.i = 0
		key := &enumerator.dataPage.Container[enumerator.i].Key
		value := &enumerator.dataPage.Container[enumerator.i].Value
		return key, value
	}
}

func (enumerator *Enumerator[TKey, V]) Previous(file *os.File) (*TKey, *V) {
	if !enumerator.HasPrevious() {
		return nil, nil
	}

	if enumerator.i >= 0 {
		key := &enumerator.dataPage.Container[enumerator.i].Key
		value := &enumerator.dataPage.Container[enumerator.i].Value
		enumerator.i--
		return key, value
	} else {
		enumerator.dataPage = ReadDataPage(enumerator.tree, file, enumerator.dataPage.Previous)
		enumerator.i = enumerator.dataPage.Count - 1
		key := &enumerator.dataPage.Container[enumerator.i].Key
		value := &enumerator.dataPage.Container[enumerator.i].Value
		enumerator.i--
		return key, value
	}
}

func (enumerator *Enumerator[TKey, V]) HasNext() bool {
	return (enumerator.dataPage.Count > 0 && enumerator.i < enumerator.dataPage.Count-1) || enumerator.dataPage.Next != -1
}

func (enumerator *Enumerator[TKey, V]) HasPrevious() bool {
	return enumerator.i > 0 || enumerator.dataPage.Previous != -1
}

func (enumerator *Enumerator[TKey, V]) Close() {
	enumerator.dataPage = nil
	enumerator.originalKeyFound = false
	enumerator.i = -1
}

// TODO implement Close to pool enumerators for a index
