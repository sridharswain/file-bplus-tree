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
		container: make([]*DataNode[TKey, TValue], tree.leafLength),
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

func (dp *DataPage[TKey, TValue]) findAndUpdateIfExists(key TKey, value TValue) (*DataNode[TKey, TValue], int, bool /*isFound*/) {
	index, found := binarySearchPage[TKey, TValue](dp.container, key)
	if found {
		// TODO update existing data
		return dp.container[index], index, true
	}
	return nil, index, false
}

func (dp *DataPage[TKey, TValue]) insert(key TKey, value TValue) bool {
	index, found := binarySearchPage[TKey, TValue](dp.container, key)

	if !found {
		// Key is not found
		dp.insertAt(index, key, value)
		return true
	} else {
		// TODO handle Found and update
		return true
	}
}

func (dp *DataPage[TKey, TValue]) insertAt(index int, key TKey, value TValue) {
	if dp.container[index] != nil {
		// if the index is not null means, there is data in the place where the ket should have been.
		copy(dp.container[index+1:], dp.container[index:])
	}
	dp.container[index] = newDataNode(key, value)
	dp.count = dp.count + 1
}

func (dp *DataPage[TKey, TValue]) deleteAt(index int) {
	dp.container[index] = nil
	dp.count = dp.count - 1
}

func (dp *DataPage[TKey, TValue]) delete(index int) {
	dp.container[index] = nil
	copy(dp.container[index:], dp.container[index+1:])
	dp.count = dp.count - 1
}

func (dp *DataPage[TKey, TValue]) split(tree *BTree[TKey, TValue]) *DataPage[TKey, TValue] {
	splitDict := newDataPage[TKey, TValue](nil, tree)

	// Create a new data page and copy second half data
	splitDict.count = copy(splitDict.container[0:], dp.container[tree.midPoint:])
	for i := tree.midPoint; i < tree.order; i++ {
		dp.deleteAt(i)
	}

	return splitDict
}
