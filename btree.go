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

func (tree *BTree[TKey, TValue]) findDataPageFromIndexRoot(key TKey) (*DataPage[TKey, TValue], int) {
	var currentPage any = tree.root

	for {
		switch indexPage := currentPage.(type) {
		case *IndexPage[TKey, TValue]:
			index, _ := binarySearchPage[TKey, TValue](indexPage.container, key)
			currentPage = indexPage.children[index]
			continue
		case *DataPage[TKey, TValue]:
			shouldBeAt, _ := binarySearchPage[TKey, TValue](indexPage.container, key)
			return indexPage, shouldBeAt
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

func (tree *BTree[TKey, TValue]) splitAndPushDataPage(key TKey, value TValue, shouldBeAt int, dataPage *DataPage[TKey, TValue]) *IndexPage[TKey, TValue] {
	newDataPage := dataPage.split(key, value, shouldBeAt, tree)

	var parent *IndexPage[TKey, TValue]

	if dataPage.parent == nil {
		parent = newIndexPage[TKey](tree)
		dataPage.parent = parent
		dataPage.parent.insertAt(0, key)
		dataPage.parent.children[0] = dataPage    // at 0 will be the old page
		dataPage.parent.children[1] = newDataPage // at 1 will be the new page
	} else {
		// If a parent already exists
		parent = dataPage.parent
		newLeafIndex, _ := dataPage.parent.insertSorted(key) // TODO check how it handles if parent is full
		parent.insertChildAt(newLeafIndex+1, newDataPage)

	}

	newDataPage.next = dataPage.next
	if newDataPage.next != nil {
		newDataPage.next.previous = dataPage
	}

	dataPage.next = newDataPage
	newDataPage.previous = dataPage

	return parent
}

func (tree *BTree[TKey, TValue]) Put(key TKey, value TValue) bool {
	switch rootNode := tree.root.(type) {
	case *DataPage[TKey, TValue]:
		shouldBeAt, isUpdated := tree.insertToLeafNode(rootNode, key, value)
		if !isUpdated {
			rootNode.insertAt(shouldBeAt, key, value)
			rootPage := tree.splitAndPushDataPage(key, value, shouldBeAt, rootNode)
			tree.root = rootPage
		}
	case *IndexPage[TKey, TValue]:
		// Find data page
		dataPageToInsert, shouldBeAt := tree.findDataPageFromIndexRoot(key)
		tree.splitAndPushDataPage(key, value, shouldBeAt, dataPageToInsert)
		return false
	}
	return true
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
