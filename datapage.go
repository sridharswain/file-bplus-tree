package main

import (
	"cmp"
	"log"
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
		container: make([]*DataNode[TKey, TValue], 4),
		parent:    parent,
		next:      nil,
		previous:  nil,
	}
}

func (dp *DataPage[TKey, TValue]) isDeficient(tree *BTree[TKey, TValue]) bool {
	return dp.count < tree.minLeafCount
}

func (dp *DataPage[TKey, TValue]) isFull(tree *BTree[TKey, TValue]) bool {
	return dp.count == 4
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
		log.Println(index)
		if !found {
			if index < len(dp.container) { // Empty container
				copy(dp.container[index + 1:], dp.container[index+2:])
				dp.container[index] = newDataNode(key, value)
			} else {
				dp.container[0] = newDataNode(key, value)
			}
			dp.count++
			return true
		} else {
			// TODO handle Found and update
		}
		return false
	}
}

func (dp *DataPage[TKey, TValue]) delete(index int, tree *BTree[TKey, TValue]) {
	dp.container[index] = nil
	dp.count--
}
