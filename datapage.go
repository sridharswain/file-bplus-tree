package main

import (
	"cmp"
)

type DataNode[TKey cmp.Ordered, TValue any] struct {
	key   TKey
	value TValue
}

type DataPage[TKey cmp.Ordered, TValue any] struct {
	count          int
	container      []*DataNode[TKey, TValue]
	next, previous *DataPage[TKey, TValue]
	parent         *IndexPage[TKey, TValue]
}

func newDataNode[TKey cmp.Ordered, TValue any](key TKey, value TValue) *DataNode[TKey, TValue] {
	return &DataNode[TKey, TValue]{
		key:   key,
		value: value,
	}
}

func newDataPage[TKey cmp.Ordered, TValue any](parent *IndexPage[TKey, TValue],
	tree *BTree[TKey, TValue]) *DataPage[TKey, TValue] {

	return &DataPage[TKey, TValue]{
		count:     0,
		container: make([]*DataNode[TKey, TValue], tree.maxLeafCount),
		parent:    parent,
		next:      nil,
		previous:  nil,
	}
}

func (dp *DataPage[TKey, TValue]) isDeficient(tree *BTree[TKey, TValue]) bool {
	return dp.count < tree.minLeafCount
}

func (dp *DataPage[TKey, TValue]) isFull(tree *BTree[TKey, TValue]) bool {
	return dp.count == tree.maxLeafCount
}

func (dp *DataPage[TKey, TValue]) isLendable(tree *BTree[TKey, TValue]) bool {
	return dp.count > tree.minLeafCount
}

func (dp *DataPage[TKey, TValue]) isMergeable(tree *BTree[TKey, TValue]) bool {
	return dp.count == tree.minLeafCount
}

func (dp *DataPage[TKey, TValue]) find(key TKey) (*DataNode[TKey, TValue], bool) {
	index, found := binarySearchPage[TKey, TValue](dp.container, key)
	if found {
		return dp.container[index], true
	}
	return nil, false
}

func (dp *DataPage[TKey, TValue]) insert(key TKey, value TValue, tree *BTree[TKey, TValue]) bool {
	if dp.isFull(tree) {
		return false
	} else {
		index, found := binarySearchPage[TKey, TValue](dp.container, key)

		if !found {
			if dp.container[index] != nil {
				// if the index is not null means, there is data in the place where the ket should have been.
				copy(dp.container[index+1:], dp.container[index:])
			}
			dp.container[index] = newDataNode(key, value)
			dp.count++
		} else {
			// TODO handle Found and update
		}
		return true
	}
}

func (dp *DataPage[TKey, TValue]) delete(index int, tree *BTree[TKey, TValue]) {
	dp.container[index] = nil
	copy(dp.container[index:], dp.container[index+1:])
	dp.count--
}
