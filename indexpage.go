package main

import (
	"cmp"
)

type IndexNode[TKey cmp.Ordered, TValue any] struct {
	key TKey
}

type IndexPage[TKey cmp.Ordered, TValue any] struct {
	count          int
	container      []*IndexNode[TKey, TValue]
	next, previous *IndexPage[TKey, TValue]
	children       []any
}

func (ip *IndexPage[TKey, TValue]) find(key TKey) (*DataNode[TKey, TValue], bool) {
	index, _ := binarySearchPage[TKey, TValue](ip.container, key)
	child := ip.children[index]
	switch x := child.(type) {
	case *IndexPage[TKey, TValue]:
		return x.find(key)
	case *DataPage[TKey, TValue]:
		return x.find(key)
	}
	return nil, false
}

func (ip *IndexPage[TKey, TValue]) insert(key TKey, value TValue, tree *BTree[TKey, TValue]) {

}
