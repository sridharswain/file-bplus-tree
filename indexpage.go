package main

import (
	"cmp"
)

type IndexNode[TKey cmp.Ordered] struct {
	key TKey
}

type IndexPage[TKey cmp.Ordered, TValue any] struct {
	count          int
	container      []*IndexNode[TKey]
	next, previous *IndexPage[TKey, TValue]
	children       []any
	parent         *IndexPage[TKey, TValue]
}

func (ip *IndexPage[TKey, TValue]) find(key TKey) (*DataNode[TKey, TValue], bool) {
	index, _ := binarySearchPage[TKey, TValue](ip.container, key)
	child := ip.children[index]
	switch x := child.(type) {
	case *IndexPage[TKey, TValue]:
		return x.find(key)
	case *DataPage[TKey, TValue]:
		return x.find(key)
	}
	return nil, false
}

func newIndexNode[TKey cmp.Ordered](key TKey) *IndexNode[TKey] {
	return &IndexNode[TKey]{
		key: key,
	}
}

func newIndexPage[TKey cmp.Ordered, TValue any](tree *BTree[TKey, TValue]) *IndexPage[TKey, TValue] {

	return &IndexPage[TKey, TValue]{
		count:     0,
		container: make([]*IndexNode[TKey], tree.order),
		children:  make([]any, tree.order+1),
	}
}

func (ip *IndexPage[TKey, TValue]) isFull(tree *BTree[TKey, TValue]) bool {
	return ip.count == tree.maxIndexCount
}

func (ip *IndexPage[TKey, TValue]) insertSorted(key TKey) (int, bool) {
	index, found := binarySearchPage[TKey, TValue](ip.container, key)

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
	if ip.container[index] != nil {
		// if the index is not null means, there is data in the place where the ket should have been.
		copy(ip.container[index+1:], ip.container[index:])
	}
	ip.container[index] = newIndexNode(key)
	ip.count++
}

func (ip *IndexPage[TKey, TValue]) deleteAt(index int) {
	ip.container[index] = nil
	ip.count--
}

func (ip *IndexPage[TKey, TValue]) insertChildAt(index int, child any) {
	if ip.children[index] != nil {
		// if the index is not null means, there is data in the place where the ket should have been.
		copy(ip.children[index+1:], ip.children[index:])
	}
	ip.children[index] = child
}

func (ip *IndexPage[TKey, TValue]) deleteChildAt(index int) {
	ip.children[index] = nil
}

func (ip *IndexPage[TKey, TValue]) split(tree *BTree[TKey, TValue]) *IndexPage[TKey, TValue] {
	splitDict := newIndexPage[TKey, TValue](tree)

	// Create a new data page and copy second half data
	splitDict.count = copy(splitDict.container[0:], ip.container[tree.midPoint+1:])
	for i := tree.midPoint; i < tree.order; i++ {
		ip.deleteAt(i)
	}

	return splitDict
}

func (ip *IndexPage[TKey, TValue]) splitChildrenFrom(newIndexPage *IndexPage[TKey, TValue],
	tree *BTree[TKey, TValue]) {

	// Create a new data page and copy second half data
	copy(newIndexPage.children[0:], ip.children[tree.midPoint+1:])
	for i := tree.midPoint + 1; i < tree.order + 1; i++ {
		ip.deleteChildAt(i)
	}
}
