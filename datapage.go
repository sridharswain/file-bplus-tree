package main

import (
	"cmp"
)

type BleedDataPage[TKey cmp.Ordered, TValue any] struct {
	Container []*DataNode[TKey, TValue]
	BleedPage int
}

type DataNode[TKey cmp.Ordered, TValue any] struct {
	Key    TKey
	Value  TValue
	Exists bool
}

type DataPage[TKey cmp.Ordered, TValue any] struct {
	tree           *BTree[TKey, TValue]
	Count          int
	Container      []DataNode[TKey, TValue]
	Next, Previous int
	Parent         int
	PageType       string
	Offset         int
	bleedPage      int
}

func newDataNode[TKey cmp.Ordered, TValue any](key TKey, value TValue) DataNode[TKey, TValue] {
	return DataNode[TKey, TValue]{
		Key:    key,
		Value:  value,
		Exists: true,
	}
}

func newDataPage[TKey cmp.Ordered, TValue any](tree *BTree[TKey, TValue]) *DataPage[TKey, TValue] {

	page := &DataPage[TKey, TValue]{
		tree:      tree,
		Count:     0,
		Container: make([]DataNode[TKey, TValue], tree.LeafLength),
		Parent:    -1,
		Next:      -1,
		Previous:  -1,
		bleedPage: -1,
		PageType:  DATA_PAGE,
	}

	SaveDataPage[TKey, TValue](tree, page, tree.LatestOffset)
	tree.LatestOffset += PAGE_BLOCK_SIZE
	SaveMetadata(tree)
	return page
}

func (dp *DataPage[TKey, TValue]) isDeficient() bool {
	return dp.Count < dp.tree.MinLeafCount
}

func (dp *DataPage[TKey, TValue]) isOverflowing() bool {
	return dp.Count == dp.tree.MaxLeafCount
}

func (dp *DataPage[TKey, TValue]) isLendable() bool {
	return dp.Count > dp.tree.MinLeafCount
}

func (dp *DataPage[TKey, TValue]) isMergeable() bool {
	return dp.Count == dp.tree.MinLeafCount
}

func (dp *DataPage[TKey, TValue]) find(key TKey) (*DataNode[TKey, TValue], bool) {
	index, found := binarySearchPage[TKey, TValue](dp.Container, key)
	if found {
		return &dp.Container[index], true
	}
	return nil, false
}

func (dp *DataPage[TKey, TValue]) findAndUpdateIfExists(key TKey, value TValue) (*DataNode[TKey, TValue], int, bool /*isFound*/) {
	index, found := binarySearchPage[TKey, TValue](dp.Container, key)
	if found {
		dp.Container[index].Value = value
		SaveDataPage[TKey, TValue](dp.tree, dp, dp.Offset)
		return &dp.Container[index], index, true
	}
	return nil, index, false
}

func (dp *DataPage[TKey, TValue]) insertAt(index int, key TKey, value TValue) {
	if dp.Container[index].Exists {
		// if the index is not null means, there is data in the place where the ket should have been.
		copy(dp.Container[index+1:], dp.Container[index:])
	}
	dp.Container[index] = newDataNode(key, value)
	dp.Count++
}

func (dp *DataPage[TKey, TValue]) deleteAtIndexAndSort(index int) {
	copy(dp.Container[index:], dp.Container[index+1:])
	dp.Count--
}

func (dp *DataPage[TKey, TValue]) deleteAt(index int) {
	dp.Container[index] = DataNode[TKey, TValue]{}
	dp.Count--
}

func (dp *DataPage[TKey, TValue]) split() *DataPage[TKey, TValue] {
	splitDict := newDataPage[TKey, TValue](dp.tree)

	// Create a new data page and copy second half data
	splitDict.Count = copy(splitDict.Container[0:], dp.Container[dp.tree.MidPoint:])
	for i := dp.tree.MidPoint; i < dp.tree.Order; i++ {
		dp.deleteAt(i)
	}
	return splitDict
}
