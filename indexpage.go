package main

import (
	"cmp"
)

type IndexNode[TKey cmp.Ordered] struct {
	Key    TKey
	Exists bool
}

type IndexPage[TKey cmp.Ordered, TValue any] struct {
	tree               *BTree[TKey, TValue]
	Count              int
	Container          []IndexNode[TKey]
	Next, Previous     int
	Children           []int
	IsChildrenDataPage bool
	Parent             int
	Offset             int
	PageType           string
}

func (ip *IndexPage[TKey, TValue]) isDeficient() bool {
	return ip.Count < ip.tree.MinIndexCount
}

func newIndexNode[TKey cmp.Ordered](key TKey) IndexNode[TKey] {
	return IndexNode[TKey]{
		Key: key,
		Exists: true,
	}
}

func newIndexPage[TKey cmp.Ordered, TValue any](tree *BTree[TKey, TValue]) *IndexPage[TKey, TValue] {
	newIndexPage := &IndexPage[TKey, TValue]{
		tree:               tree,
		Count:              0,
		Container:          make([]IndexNode[TKey], tree.Order),
		Children:           make([]int, tree.Order+1),
		IsChildrenDataPage: false,
		PageType:           INDEX_PAGE,
		Parent:             -1,
		Next:               -1,
		Previous:           -1,
	}

	for i := 0; i < len(newIndexPage.Children); i++ {
		newIndexPage.Children[i] = -1
	}

	SaveIndexPage[TKey, TValue](tree, newIndexPage, tree.LatestOffset)
	tree.LatestOffset += INDEX_BLOCK_SIZE
	SaveMetadata(tree)
	return newIndexPage
}

func (ip *IndexPage[TKey, TValue]) isFull() bool {
	return ip.Count == ip.tree.MaxIndexCount
}

func (ip *IndexPage[TKey, TValue]) insertSorted(key TKey) (int, bool) {
	index, found := binarySearchPage[TKey, TValue](ip.Container, key)

	if !found {
		// Key is not found
		ip.insertAt(index, key)
		return index, true
	} else {
		// TODO handle Found and update
		return index, true
	}
}

func (ip *IndexPage[TKey, TValue]) insertAt(index int, key TKey) {
	if ip.Container[index].Exists {
		// if the index is not null means, there is data in the place where the ket should have been.
		copy(ip.Container[index+1:], ip.Container[index:])
	}
	ip.Container[index] = newIndexNode(key)
	ip.Count++
}

func (ip *IndexPage[TKey, TValue]) deleteAt(index int) {
	ip.Container[index] = IndexNode[TKey]{}
	ip.Count--
}

func (ip *IndexPage[TKey, TValue]) insertChildAt(index int, child int) {
	if ip.Children[index] != -1 {
		// if the index is not null means, there is data in the place where the ket should have been.
		copy(ip.Children[index+1:], ip.Children[index:])
	}
	ip.Children[index] = child
}

func (ip *IndexPage[TKey, TValue]) deleteChildAt(index int) {
	ip.Children[index] = -1
}

func (ip *IndexPage[TKey, TValue]) split() *IndexPage[TKey, TValue] {
	splitDict := newIndexPage[TKey, TValue](ip.tree)

	// Create a new data page and copy second half data
	splitDict.Count = copy(splitDict.Container[0:], ip.Container[ip.tree.MidPoint+1:])
	splitDict.IsChildrenDataPage = ip.IsChildrenDataPage
	for i := ip.tree.MidPoint; i < ip.tree.Order; i++ {
		ip.deleteAt(i)
	}
	return splitDict
}

func (ip *IndexPage[TKey, TValue]) splitChildrenFrom(newIndexPage *IndexPage[TKey, TValue],
	tree *BTree[TKey, TValue]) {

	// Create a new data page and copy second half data
	copy(newIndexPage.Children[0:], ip.Children[tree.MidPoint+1:])
	for i := tree.MidPoint + 1; i < tree.Order+1; i++ {
		ip.deleteChildAt(i)
	}
}

func (ip *IndexPage[TKey, TValue]) deleteAtAndSort(index int) {
	copy(ip.Container[index:], ip.Container[index+1:])
	ip.Count--
}
