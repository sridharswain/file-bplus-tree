package btree

import (
	"bptree/utils"
	"os"
)

type IndexNode[TKey any] struct {
	Key    TKey
	Exists bool
}

type IndexPage[TKey, TValue any] struct {
	tree               *BTree[TKey, TValue]
	Count              int
	Container          []IndexNode[TKey]
	Next, Previous     int
	Children           []int
	IsChildrenDataPage bool
	Parent             int
	Offset             int
}

func (ip *IndexPage[TKey, TValue]) isDeficient() bool {
	return ip.Count < ip.tree.MinIndexCount
}

func (ip *IndexPage[TKey, TValue]) isLendable() bool {
	return ip.Count > ip.tree.MinIndexCount
}

func (ip *IndexPage[TKey, TValue]) isMergeable() bool {
	return ip.Count == ip.tree.MinIndexCount
}

func (ip *IndexPage[TKey, TValue]) clear() {
	ip.Container = make([]IndexNode[TKey], ip.tree.Order)
	ip.Children = make([]int, ip.tree.Order+1)
	ip.Next = 0
	ip.Previous = 0
	ip.Count = 0
}

func newIndexNode[TKey any](key TKey) IndexNode[TKey] {
	return IndexNode[TKey]{
		Key:    key,
		Exists: true,
	}
}

func newIndexPage[TKey, TValue any](tree *BTree[TKey, TValue], file *os.File) *IndexPage[TKey, TValue] {
	newIndexPage := &IndexPage[TKey, TValue]{
		tree:               tree,
		Count:              0,
		Container:          make([]IndexNode[TKey], tree.Order),
		Children:           make([]int, tree.Order+1),
		IsChildrenDataPage: false,
		Parent:             -1,
		Next:               -1,
		Previous:           -1,
	}

	for i := 0; i < len(newIndexPage.Children); i++ {
		newIndexPage.Children[i] = -1
	}

	SaveIndexPage[TKey, TValue](tree, newIndexPage, file, tree.LatestOffset)
	tree.LatestOffset += IndexBlockSize
	SaveMetadata(tree, file)
	return newIndexPage
}

func (ip *IndexPage[TKey, TValue]) isOverflowing() bool {
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

func (ip *IndexPage[TKey, TValue]) split(file *os.File) *IndexPage[TKey, TValue] {
	splitDict := newIndexPage[TKey, TValue](ip.tree, file)

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

func (ip *IndexPage[TKey, TValue]) deleteAtIndexAndSort(index int) {
	copy(ip.Container[index:], ip.Container[index+1:])
	ip.Count--
}

func (ip *IndexPage[TKey, TValue]) getRangesIn(sortedKeys []TKey, lower, upper int) map[int][2]int {
	var result = make(map[int][2]int)

	var keyPointer int = lower
	var indexPointer int = 0

	var start = lower

	for keyPointer < upper && indexPointer < ip.Count {
		if utils.Compare(ip.Container[indexPointer].Key, sortedKeys[keyPointer]) < 0 {
			result[indexPointer] = [2]int{start, keyPointer + 1}
			start = keyPointer + 1
			indexPointer++
		} else {
			keyPointer++
		}
	}

	if keyPointer < upper {
		result[indexPointer] = [2]int{start, upper}
	}

	return result
}
