package btree

import "os"

type BleedDataPage[TKey, TValue any] struct {
	Container []*DataNode[TKey, TValue]
	BleedPage int
}

type DataNode[TKey, TValue any] struct {
	Key    TKey
	Value  TValue
	Exists bool
}

type DataPage[TKey, TValue any] struct {
	tree           *BTree[TKey, TValue]
	Count          int
	Container      []DataNode[TKey, TValue]
	Next, Previous int
	Parent         int
	Offset         int
	bleedPage      int
}

func newDataNode[TKey, TValue any](key TKey, value TValue) DataNode[TKey, TValue] {
	return DataNode[TKey, TValue]{
		Key:    key,
		Value:  value,
		Exists: true,
	}
}

func newDataPage[TKey, TValue any](tree *BTree[TKey, TValue], file *os.File) *DataPage[TKey, TValue] {

	page := &DataPage[TKey, TValue]{
		tree:      tree,
		Count:     0,
		Container: make([]DataNode[TKey, TValue], tree.LeafLength),
		Parent:    -1,
		Next:      -1,
		Previous:  -1,
		bleedPage: -1,
	}

	SaveDataPage[TKey, TValue](tree, page, file, tree.LatestOffset)
	tree.LatestOffset += PageBlockSize
	SaveMetadata(tree, file)
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

func (dp *DataPage[TKey, TValue]) findAndUpdateIfExists(key TKey, file *os.File, value TValue) (*DataNode[TKey, TValue], int, bool /*isFound*/) {
	index, found := binarySearchPage[TKey, TValue](dp.Container, key)
	if found {
		dp.Container[index].Value = value
		SaveDataPage[TKey, TValue](dp.tree, dp, file, dp.Offset)
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

func (dp *DataPage[TKey, TValue]) split(file *os.File) *DataPage[TKey, TValue] {
	splitDict := newDataPage[TKey, TValue](dp.tree, file)

	// Create a new data page and copy second half data
	splitDict.Count = copy(splitDict.Container[0:], dp.Container[dp.tree.MidPoint:])
	for i := dp.tree.MidPoint; i < dp.tree.Order; i++ {
		dp.deleteAt(i)
	}
	return splitDict
}
