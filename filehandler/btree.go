package main

import (
	"cmp"
	"math"
)

type TPage[TKey cmp.Ordered, TValue any] interface {
	*DataPage[TKey, TValue] | *IndexPage[TKey, TValue]
}

type TNode[TKey cmp.Ordered, TValue any] interface {
	*DataNode[TKey, TValue] | *IndexNode[TKey]
}

type BTree[TKey cmp.Ordered, TValue any] struct {
	indexName string
	Count     int
	Order     int

	LeafLength   int
	MinLeafCount int
	MaxLeafCount int

	MinIndexCount int
	MaxIndexCount int

	MidPoint int

	root       any
	RootOffset int

	First, Last int

	LatestOffset int
}

func New[TKey cmp.Ordered, TValue any](indexName string, order int) *BTree[TKey, TValue] {
	newTree := &BTree[TKey, TValue]{
		RootOffset: METADATA_SIZE,
		indexName:  indexName,
		Count:      0,
		Order:      order,
		MidPoint:   int(math.Ceil((float64(order)+1)/2.0) - 1),

		LeafLength:   order,
		MaxLeafCount: order - 1,
		MinLeafCount: int(math.Ceil(float64(order)/2.0) - 1),

		MaxIndexCount: order,
		MinIndexCount: int(math.Ceil(float64(order) / 2.0)),

		// TODO update First and last
	}
	SaveMetadata[TKey, TValue](indexName, newTree)
	newTree.LatestOffset += METADATA_SIZE
	newTree.root = newDataPage[TKey, TValue](newTree)
	return newTree
}

// func (tree *BTree[TKey, TValue]) findDataPageFromIndexRoot(key TKey) *DataPage[TKey, TValue] {
// 	var currentPage int = tree.RootOffset

// 	for {
// 		switch indexPage := currentPage.(type) {
// 		case *IndexPage[TKey, TValue]:
// 			index, _ := binarySearchPage[TKey, TValue](indexPage.Container, key)
// 			currentPage = indexPage.children[index]
// 			continue
// 		case *DataPage[TKey, TValue]:
// 			return indexPage
// 		}
// 	}
// }

func (tree *BTree[TKey, TValue]) insertToLeafNode(dataPage *DataPage[TKey, TValue], key TKey, value TValue) (int, bool /*isFull*/) {
	_, shouldBeAt, alreadyExists := dataPage.findAndUpdateIfExists(key, value)

	if alreadyExists {
		// TODO Handle Updated
		return shouldBeAt, false
	} else {
		if dataPage.isFull() {
			return shouldBeAt, true
		} else {
			dataPage.insertAt(shouldBeAt, key, value)
			return shouldBeAt, false
		}
	}
}

// func (tree *BTree[TKey, TValue]) splitAndPushIndexPage(indexPage *IndexPage[TKey, TValue]) *IndexPage[TKey, TValue] {
// 	parent := indexPage.parent
// 	newParentKey := indexPage.container[tree.midPoint]

// 	newIndexHalf := indexPage.split(tree)
// 	indexPage.splitChildrenFrom(newIndexHalf, tree)

// 	for _, child := range newIndexHalf.children {
// 		if child != nil {
// 			switch x := child.(type) {
// 			case *IndexPage[TKey, TValue]:
// 				x.parent = newIndexHalf
// 			case *DataPage[TKey, TValue]:
// 				x.parent = newIndexHalf
// 			}
// 		}
// 	}

// 	newIndexHalf.next = indexPage.next
// 	if newIndexHalf.next != nil {
// 		newIndexHalf.next.previous = newIndexHalf
// 	}
// 	indexPage.next = newIndexHalf
// 	newIndexHalf.previous = indexPage

// 	if parent == nil {
// 		parent = newIndexPage[TKey](tree)
// 		parent.insertAt(0, newParentKey.key)
// 		parent.insertChildAt(0, indexPage)
// 		parent.insertChildAt(1, newIndexHalf)
// 		indexPage.parent = parent

// 		tree.root = parent
// 	} else {
// 		insertedAt, _ := parent.insertSorted(newParentKey.key)
// 		parent.insertChildAt(insertedAt+1, newIndexHalf)
// 	}
// 	newIndexHalf.parent = parent

// 	return parent
// }

func (tree *BTree[TKey, TValue]) splitAndPushDataPage(dataPage *DataPage[TKey, TValue]) *IndexPage[TKey, TValue] {
	newDataPage := dataPage.split()

	var parent *IndexPage[TKey, TValue]

	if dataPage.Parent == -1 {
		parent = newIndexPage[TKey](tree)
		dataPage.Parent = parent.Offset
		parent.insertAt(0, newDataPage.Container[0].Key)

		parent.insertChildAt(0, dataPage.Offset)    // at 0 will be the old page
		parent.insertChildAt(1, newDataPage.Offset) // at 1 will be the new page
		SaveIndexPage[TKey, TValue](tree.indexName, parent, parent.Offset)
	} else {
		// If a parent already exists
		// parent = dataPage.parent
		// newLeafIndex, _ := parent.insertSorted(newDataPage.container[0].key) // TODO check how it handles if parent is full
		// parent.insertChildAt(newLeafIndex+1, newDataPage)
	}

	// Set parent for the new page
	newDataPage.Parent = parent.Offset

	newDataPage.Next = dataPage.Next
	if newDataPage.Next != -1 {
		var nextDataPage DataPage[TKey, TValue]
		ReadDataPage[TKey, TValue](tree, &nextDataPage, newDataPage.Next)
		nextDataPage.Previous = dataPage.Offset
		SaveDataPage[TKey, TValue](tree.indexName, &nextDataPage, nextDataPage.Offset)
	}

	dataPage.Next = newDataPage.Offset
	newDataPage.Previous = dataPage.Offset
	SaveDataPage[TKey, TValue](tree.indexName, dataPage, dataPage.Offset)
	SaveDataPage[TKey, TValue](tree.indexName, newDataPage, newDataPage.Offset)

	// currentParent := parent
	// for currentParent != nil {
	// 	if currentParent.isFull() {
	// 		currentParent = tree.splitAndPushIndexPage(currentParent)
	// 	} else {
	// 		break
	// 	}
	// }

	return parent
}

func (tree *BTree[TKey, TValue]) Put(key TKey, value TValue) {
	switch rootNode := tree.root.(type) {
	case *DataPage[TKey, TValue]:
		shouldBeAt, isFull := tree.insertToLeafNode(rootNode, key, value)
		if isFull {
			rootNode.insertAt(shouldBeAt, key, value)
			rootPage := tree.splitAndPushDataPage(rootNode)
			tree.RootOffset = rootPage.Offset
			SaveMetadata[TKey, TValue](tree.indexName, tree)
		}
	case *IndexPage[TKey, TValue]:
		// Find data page
		// dataPageToInsert := tree.findDataPageFromIndexRoot(key)
		// shouldBeAt, isFull := tree.insertToLeafNode(dataPageToInsert, key, value)
		// if isFull {
		// 	dataPageToInsert.insertAt(shouldBeAt, key, value)
		// 	// tree.splitAndPushDataPage(dataPageToInsert)
		// }
	}
}

// func (tree *BTree[TKey, TValue]) Get(key TKey) (value *TValue, exists bool) {
// 	dataPage := tree.findDataPageFromIndexRoot(key)
// 	dataNodeIndex, found := binarySearchPage[TKey, TValue](dataPage.Container, key)

// 	if found {
// 		return &dataPage.Container[dataNodeIndex].Value, true
// 	}
// 	return nil, false
// }

func (tree *BTree[TKey, TValue]) Seek(key TKey) (value TValue, exists bool) {
	return
}

func (tree *BTree[TKey, TValue]) Delete() (ok bool) {
	return
}
