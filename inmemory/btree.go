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
	count int
	order int

	leafLength   int
	minLeafCount int
	maxLeafCount int

	minIndexCount int
	maxIndexCount int

	midPoint int

	root any

	first, last *DataPage[TKey, TValue]
}

func New[TKey cmp.Ordered, TValue any](order int) *BTree[TKey, TValue] {
	newTree := &BTree[TKey, TValue]{
		count:    0,
		order:    order,
		midPoint: int(math.Ceil((float64(order)+1)/2.0) - 1),

		leafLength:   order,
		maxLeafCount: order - 1,
		minLeafCount: int(math.Ceil(float64(order)/2.0) - 1),

		maxIndexCount: order,
		minIndexCount: int(math.Ceil(float64(order) / 2.0)),
		// TODO update First and last
	}
	newTree.root = newDataPage[TKey, TValue](nil, newTree)
	return newTree
}

func (tree *BTree[TKey, TValue]) findDataPageFromIndexRoot(key TKey) *DataPage[TKey, TValue] {
	var currentPage any = tree.root

	for {
		switch indexPage := currentPage.(type) {
		case *IndexPage[TKey, TValue]:
			index, _ := binarySearchPage[TKey, TValue](indexPage.container, key)
			
			currentPage = indexPage.children[index]
			continue
		case *DataPage[TKey, TValue]:
			return indexPage
		}
	}
}

func (tree *BTree[TKey, TValue]) insertToLeafNode(dataPage *DataPage[TKey, TValue], key TKey, value TValue) (int, bool /*isSuccessfullyUpdated*/) {
	_, shouldBeAt, alreadyExists := dataPage.findAndUpdateIfExists(key, value)

	if alreadyExists {
		// TODO Handle Updated
		return shouldBeAt, true
	} else {
		if dataPage.isFull(tree) {
			return shouldBeAt, false
		} else {
			dataPage.insertAt(shouldBeAt, key, value)
			return shouldBeAt, true
		}
	}
}

func (tree *BTree[TKey, TValue]) splitAndPushIndexPage(indexPage *IndexPage[TKey, TValue]) *IndexPage[TKey, TValue] {
	parent := indexPage.parent
	newParentKey := indexPage.container[tree.midPoint]

	newIndexHalf := indexPage.split(tree)
	indexPage.splitChildrenFrom(newIndexHalf, tree)

	for _, child := range newIndexHalf.children {
		if child != nil {
			switch x := child.(type) {
			case *IndexPage[TKey, TValue]:
				x.parent = newIndexHalf
			case *DataPage[TKey, TValue]:
				x.parent = newIndexHalf
			}
		}
	}

	newIndexHalf.next = indexPage.next
	if newIndexHalf.next != nil {
		newIndexHalf.next.previous = newIndexHalf
	}
	indexPage.next = newIndexHalf
	newIndexHalf.previous = indexPage

	if parent == nil {
		parent = newIndexPage[TKey](tree)
		parent.insertAt(0, newParentKey.key)
		parent.insertChildAt(0, indexPage)
		parent.insertChildAt(1, newIndexHalf)
		indexPage.parent = parent

		tree.root = parent
	} else {
		insertedAt, _ := parent.insertSorted(newParentKey.key)
		parent.insertChildAt(insertedAt+1, newIndexHalf)
	}
	newIndexHalf.parent = parent

	return parent
}

func (tree *BTree[TKey, TValue]) splitAndPushDataPage(dataPage *DataPage[TKey, TValue]) *IndexPage[TKey, TValue] {
	newDataPage := dataPage.split(tree)

	var parent *IndexPage[TKey, TValue]

	if dataPage.parent == nil {
		parent = newIndexPage[TKey](tree)
		dataPage.parent = parent
		dataPage.parent.insertAt(0, newDataPage.container[0].key)

		dataPage.parent.insertChildAt(0, dataPage)    // at 0 will be the old page
		dataPage.parent.insertChildAt(1, newDataPage) // at 1 will be the new page
	} else {
		// If a parent already exists
		parent = dataPage.parent
		newLeafIndex, _ := parent.insertSorted(newDataPage.container[0].key) // TODO check how it handles if parent is full
		parent.insertChildAt(newLeafIndex+1, newDataPage)
	}

	// Set parent for the new page
	newDataPage.parent = parent

	newDataPage.next = dataPage.next
	if newDataPage.next != nil {
		newDataPage.next.previous = dataPage
	}

	dataPage.next = newDataPage
	newDataPage.previous = dataPage

	currentParent := parent
	for currentParent != nil {
		if currentParent.isFull(tree) {
			currentParent = tree.splitAndPushIndexPage(currentParent)
		} else {
			break
		}
	}

	return parent
}

func (tree *BTree[TKey, TValue]) Put(key TKey, value TValue) {
	switch rootNode := tree.root.(type) {
	case *DataPage[TKey, TValue]:
		shouldBeAt, isUpdated := tree.insertToLeafNode(rootNode, key, value)
		if !isUpdated {
			rootNode.insertAt(shouldBeAt, key, value)
			rootPage := tree.splitAndPushDataPage(rootNode)
			tree.root = rootPage
		}
	case *IndexPage[TKey, TValue]:
		// Find data page
		dataPageToInsert := tree.findDataPageFromIndexRoot(key)
		shouldBeAt, isUpdated := tree.insertToLeafNode(dataPageToInsert, key, value)
		if !isUpdated {
			dataPageToInsert.insertAt(shouldBeAt, key, value)
			tree.splitAndPushDataPage(dataPageToInsert)
		}
	}
}

func (tree *BTree[TKey, TValue]) Get(key TKey) (value *TValue, exists bool) {
	dataPage := tree.findDataPageFromIndexRoot(key)
	dataNodeIndex, found := binarySearchPage[TKey, TValue](dataPage.container, key)

	if found {
		return &dataPage.container[dataNodeIndex].value, true
	}
	return nil, false
}

func (tree *BTree[TKey, TValue]) Seek(key TKey) (value TValue, exists bool) {
	return
}

func (tree *BTree[TKey, TValue]) Delete() (ok bool) {
	return
}
