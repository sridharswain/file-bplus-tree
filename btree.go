package main

import (
	"cmp"
	"slices"
)

type TPage[TKey cmp.Ordered, TValue any] interface {
	*DataPage[TKey, TValue] | *IndexPage[TKey, TValue]
}

type TNode[TKey cmp.Ordered, TValue any] interface {
	*DataNode[TKey, TValue] | *IndexNode[TKey, TValue]
}

func binarySearchPage[TKey cmp.Ordered, TValue any, TTNode TNode[TKey, TValue]](space []TTNode, key TKey) (int, bool) {
	return slices.BinarySearchFunc(space, key, func(t1 TTNode, t2 TKey) int {
		if t1 == nil {
			return +1
		}
		switch x := any(t1).(type) {
		case *DataNode[TKey, TValue]:
			return cmp.Compare(x.key, t2)
		case *IndexNode[TKey, TValue]:
			return cmp.Compare(x.key, t2)
		}
		return -1
	})
}

type BTree[TKey cmp.Ordered, TValue any] struct {
	count int
	order int

	minLeafCount int
	maxLeafCount int

	minIndexCount int
	maxIndexCount int

	first, last *DataPage[TKey, TValue]
}

func New[TKey cmp.Ordered, TValue any](order int) *BTree[TKey, TValue] {
	return &BTree[TKey, TValue]{
		count: 0,
		order: order,
		// TODO update First and last
	}
}

func (tree *BTree[TKey, TValue]) Put(key TKey, value TValue) {

}

func (tree *BTree[TKey, TValue]) Get(key TKey) (value *TValue, exists bool) {
	return
}

func (tree *BTree[TKey, TValue]) Seek(key TKey) (value *TValue, exists bool) {
	return
}

func (tree *BTree[TKey, TValue]) Delete() (ok bool) {
	return
}
