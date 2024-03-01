package main

import (
	"cmp"
)

type IndexNode[TKey cmp.Ordered] struct {
	Key TKey
}

type IndexPage[TKey cmp.Ordered, TValue any] struct {
	tree           *BTree[TKey, TValue]
	Count          int
	Container      []*IndexNode[TKey]
	Next, Previous int
	Children       []int
	Parent         int
	Offset         int
	PageType       string
}

// func (ip *IndexPage[TKey, TValue]) find(key TKey) (*DataNode[TKey, TValue], bool) {
// 	index, _ := binarySearchPage[TKey, TValue](ip.container, key)
// 	child := ip.children[index]
// 	switch x := child.(type) {
// 	case *IndexPage[TKey, TValue]:
// 		return x.find(key)
// 	case *DataPage[TKey, TValue]:
// 		return x.find(key)
// 	}
// 	return nil, false
// }

func newIndexNode[TKey cmp.Ordered](key TKey) *IndexNode[TKey] {
	return &IndexNode[TKey]{
		Key: key,
	}
}

func newIndexPage[TKey cmp.Ordered, TValue any](tree *BTree[TKey, TValue]) *IndexPage[TKey, TValue] {
	newIndexPage := &IndexPage[TKey, TValue]{
		tree:      tree,
		Count:     0,
		Container: make([]*IndexNode[TKey], tree.Order),
		Children:  make([]int, tree.Order+1),
		PageType:  INDEX_PAGE,
		Parent:    -1,
		Next:      -1,
		Previous:  -1,
	}

	for i := 0; i < len(newIndexPage.Children); i++ {
		newIndexPage.Children[i] = -1
	}

	SaveIndexPage[TKey, TValue](tree.indexName, newIndexPage, tree.LatestOffset)
	tree.LatestOffset += INDEX_BLOCK_SIZE
	return newIndexPage
}

func (ip *IndexPage[TKey, TValue]) isFull() bool {
	return ip.Count == ip.tree.MaxIndexCount
}

// func (ip *IndexPage[TKey, TValue]) insertSorted(key TKey) (int, bool) {
// 	index, found := binarySearchPage[TKey, TValue](ip.container, key)

// 	if !found {
// 		// Key is not found
// 		ip.insertAt(index, key)
// 		return index, true
// 	} else {
// 		// TODO handle Found and update
// 		return index, true
// 	}
// }

func (ip *IndexPage[TKey, TValue]) insertAt(index int, key TKey) {
	if ip.Container[index] != nil {
		// if the index is not null means, there is data in the place where the ket should have been.
		copy(ip.Container[index+1:], ip.Container[index:])
	}
	ip.Container[index] = newIndexNode(key)
	ip.Count++
	SaveIndexPage[TKey, TValue](ip.tree.indexName, ip, ip.Offset)
}

// func (ip *IndexPage[TKey, TValue]) deleteAt(index int) {
// 	ip.container[index] = nil
// 	ip.count--
// }

func (ip *IndexPage[TKey, TValue]) insertChildAt(index int, child int) {
	if ip.Children[index] != -1 {
		// if the index is not null means, there is data in the place where the ket should have been.
		copy(ip.Children[index+1:], ip.Children[index:])
	}
	ip.Children[index] = child
}

// func (ip *IndexPage[TKey, TValue]) deleteChildAt(index int) {
// 	ip.children[index] = nil
// }

// func (ip *IndexPage[TKey, TValue]) split(tree *BTree[TKey, TValue]) *IndexPage[TKey, TValue] {
// 	splitDict := newIndexPage[TKey, TValue](tree)

// 	// Create a new data page and copy second half data
// 	splitDict.count = copy(splitDict.container[0:], ip.container[tree.midPoint+1:])
// 	for i := tree.midPoint; i < tree.order; i++ {
// 		ip.deleteAt(i)
// 	}

// 	return splitDict
// }

// func (ip *IndexPage[TKey, TValue]) splitChildrenFrom(newIndexPage *IndexPage[TKey, TValue],
// 	tree *BTree[TKey, TValue]) {

// 	// Create a new data page and copy second half data
// 	copy(newIndexPage.children[0:], ip.children[tree.midPoint+1:])
// 	for i := tree.midPoint + 1; i < tree.order+1; i++ {
// 		ip.deleteChildAt(i)
// 	}
// }
