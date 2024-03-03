package main

import (
	"cmp"
	"math"
)

type TPage[TKey cmp.Ordered, TValue any] interface {
	*DataPage[TKey, TValue] | *IndexPage[TKey, TValue]
}

type TNode[TKey cmp.Ordered, TValue any] interface {
	DataNode[TKey, TValue] | IndexNode[TKey]
}

type BTree[TKey cmp.Ordered, TValue any] struct {
	IndexName string
	Count     int
	Order     int

	LeafLength   int
	MinLeafCount int
	MaxLeafCount int

	MinIndexCount int
	MaxIndexCount int

	MidPoint int

	RootOffset int

	First, Last int

	IsLeaf       bool
	LatestOffset int
}

func New[TKey cmp.Ordered, TValue any](indexName string, order int) *BTree[TKey, TValue] {

	if indexFileExists(indexName) {
		return ReadMetadata[TKey, TValue](indexName)
	} else {
		newTree := &BTree[TKey, TValue]{
			RootOffset: METADATA_SIZE,
			IndexName:  indexName,
			Count:      0,
			Order:      order,
			MidPoint:   int(math.Ceil((float64(order)+1)/2.0) - 1),

			LeafLength:   order,
			MaxLeafCount: order - 1,
			MinLeafCount: int(math.Ceil(float64(order)/2.0) - 1),

			MaxIndexCount: order,
			MinIndexCount: int(math.Ceil(float64(order) / 2.0)),
			IsLeaf:        true,
			// TODO update First and last
		}
		newTree.LatestOffset += METADATA_SIZE
		newDataPage[TKey, TValue](newTree)  // Create a lead data page for inital ops
		return newTree
	}
}

func (tree *BTree[TKey, TValue]) findDataPageFromIndexRoot(key TKey) *DataPage[TKey, TValue] {
	var currentPageOffset int = tree.RootOffset

	if tree.IsLeaf {
		rootDataPage := ReadDataPage(tree, currentPageOffset)
		return rootDataPage
	}

	for {
		currentIndexPage := ReadIndexPage(tree, currentPageOffset)
		index, _ := binarySearchPage[TKey, TValue](currentIndexPage.Container, key)

		if currentIndexPage.IsChildrenDataPage {
			dataPage := ReadDataPage(tree, currentIndexPage.Children[index])
			return dataPage
		} else {
			currentPageOffset = currentIndexPage.Children[index]
		}
	}
}

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
			SaveDataPage[TKey, TValue](tree, dataPage, dataPage.Offset)
			return shouldBeAt, false
		}
	}
}

func (tree *BTree[TKey, TValue]) splitAndPushIndexPage(indexPage *IndexPage[TKey, TValue]) *IndexPage[TKey, TValue] {
	parentOffset := indexPage.Parent
	newParentKey := indexPage.Container[tree.MidPoint]

	newIndexHalf := indexPage.split()
	indexPage.splitChildrenFrom(newIndexHalf, tree)

	for _, child := range newIndexHalf.Children {
		if child != -1 {
			if newIndexHalf.IsChildrenDataPage {
				childDataPage := ReadDataPage(tree, child)
				childDataPage.Parent = newIndexHalf.Offset
				SaveDataPage(tree, childDataPage, childDataPage.Offset)
			} else {
				childIndexPage := ReadIndexPage(tree, child)
				childIndexPage.Parent = newIndexHalf.Offset
				SaveIndexPage(tree, childIndexPage, childIndexPage.Offset)
			}
		} else {
			break
		}
	}

	newIndexHalf.Next = indexPage.Next
	if newIndexHalf.Next != -1 {
		nextIndexSibling := ReadIndexPage(tree, newIndexHalf.Next)
		nextIndexSibling.Previous = newIndexHalf.Offset
		SaveIndexPage(tree, nextIndexSibling, nextIndexSibling.Offset)
	}
	indexPage.Next = newIndexHalf.Offset
	newIndexHalf.Previous = indexPage.Offset

	var parentIndexPage *IndexPage[TKey, TValue]
	if parentOffset == -1 {
		parentIndexPage = newIndexPage[TKey](tree)
		parentIndexPage.insertAt(0, newParentKey.Key)
		parentIndexPage.insertChildAt(0, indexPage.Offset)
		parentIndexPage.insertChildAt(1, newIndexHalf.Offset)
		indexPage.Parent = parentIndexPage.Offset
		parentOffset = parentIndexPage.Offset
		tree.RootOffset = parentIndexPage.Offset
		SaveMetadata(tree)
	} else {
		parentIndexPage = ReadIndexPage(tree, parentOffset)
		insertedAt, _ := parentIndexPage.insertSorted(newParentKey.Key)
		parentIndexPage.insertChildAt(insertedAt+1, newIndexHalf.Offset)
	}
	newIndexHalf.Parent = parentOffset
	SaveIndexPage(tree, indexPage, indexPage.Offset)
	SaveIndexPage(tree, parentIndexPage, parentIndexPage.Offset)
	SaveIndexPage(tree, newIndexHalf, newIndexHalf.Offset)

	return parentIndexPage
}

func (tree *BTree[TKey, TValue]) splitAndPushDataPage(dataPage *DataPage[TKey, TValue]) *IndexPage[TKey, TValue] {
	newDataPage := dataPage.split()

	var parent *IndexPage[TKey, TValue]

	if dataPage.Parent == -1 {
		parent = newIndexPage[TKey](tree)
		dataPage.Parent = parent.Offset
		parent.IsChildrenDataPage = true
		parent.insertAt(0, newDataPage.Container[0].Key)

		parent.insertChildAt(0, dataPage.Offset)    // at 0 will be the old page
		parent.insertChildAt(1, newDataPage.Offset) // at 1 will be the new page
	} else {
		// If a parent already exists
		parent = ReadIndexPage(tree, dataPage.Parent)
		newLeafIndex, _ := parent.insertSorted(newDataPage.Container[0].Key) // TODO check how it handles if parent is full
		parent.insertChildAt(newLeafIndex+1, newDataPage.Offset)
	}
	SaveIndexPage[TKey, TValue](tree, parent, parent.Offset)

	// Set parent for the new page
	newDataPage.Parent = parent.Offset

	newDataPage.Next = dataPage.Next
	if newDataPage.Next != -1 {
		nextDataPage := ReadDataPage[TKey, TValue](tree, newDataPage.Next)
		nextDataPage.Previous = dataPage.Offset
		SaveDataPage[TKey, TValue](tree, nextDataPage, nextDataPage.Offset)
	}

	dataPage.Next = newDataPage.Offset
	newDataPage.Previous = dataPage.Offset
	SaveDataPage[TKey, TValue](tree, dataPage, dataPage.Offset)
	SaveDataPage[TKey, TValue](tree, newDataPage, newDataPage.Offset)

	currentParent := parent
	for currentParent != nil {
		if currentParent.isFull() {
			currentParent = tree.splitAndPushIndexPage(currentParent)
		} else {
			break
		}
	}

	return parent
}

func (tree *BTree[TKey, TValue]) Put(key TKey, value TValue) {
	if tree.IsLeaf {
		rootNode := ReadDataPage(tree, tree.RootOffset)
		shouldBeAt, isFull := tree.insertToLeafNode(rootNode, key, value)
		if isFull {
			rootNode.insertAt(shouldBeAt, key, value)
			rootPage := tree.splitAndPushDataPage(rootNode)
			tree.RootOffset = rootPage.Offset
			tree.IsLeaf = false
			SaveMetadata[TKey, TValue](tree)
		}
	} else {
		// Find data page
		dataPageToInsert := tree.findDataPageFromIndexRoot(key)
		shouldBeAt, isFull := tree.insertToLeafNode(dataPageToInsert, key, value)
		if isFull {
			dataPageToInsert.insertAt(shouldBeAt, key, value)
			tree.splitAndPushDataPage(dataPageToInsert)
		}
	}
}

func (tree *BTree[TKey, TValue]) Get(key TKey) (*TValue, bool) {
	dataPage := tree.findDataPageFromIndexRoot(key)
	dataNodeIndex, found := binarySearchPage[TKey, TValue](dataPage.Container, key)

	if found {
		return &dataPage.Container[dataNodeIndex].Value, true
	}
	return nil, false
}

func (tree *BTree[TKey, TValue]) Seek(key TKey) (value TValue, exists bool) {
	return
}

func (tree *BTree[TKey, TValue]) Delete() (ok bool) {
	return
}
