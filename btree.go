package main

import (
	"cmp"
	"log"
	"math"
	"os"
)

type TPage[TKey cmp.Ordered, TValue any] interface {
	*DataPage[TKey, TValue] | *IndexPage[TKey, TValue]
}

type TNode[TKey cmp.Ordered, TValue any] interface {
	DataNode[TKey, TValue] | IndexNode[TKey]
}

type BTree[TKey cmp.Ordered, TValue any] struct {
	IndexName string

	Count int
	Order int

	LeafLength   int
	MinLeafCount int
	MaxLeafCount int

	MinIndexCount int
	MaxIndexCount int

	MidPoint int

	RootOffset int

	First, Last int

	LatestOffset int
	IsLeaf       bool
}

func (tree *BTree[TKey, TValue]) IsEmpty() bool {
	return tree.Count == 0
}

func indexFileExists(indexName string) bool {
	if _, err := os.Stat(indexName); err == nil {
		return true
	}

	return false
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
			MinIndexCount: int(math.Ceil(float64(order)/2.0) - 1),
			IsLeaf:        true,
			// TODO update First and last
		}
		newTree.LatestOffset += METADATA_SIZE
		newDataPage(newTree) // Create a leaf data page for inital ops
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
		index, found := binarySearchPage[TKey, TValue](currentIndexPage.Container, key)

		if currentIndexPage.IsChildrenDataPage {
			if found {
				dataPage := ReadDataPage(tree, currentIndexPage.Children[index+1])
				return dataPage
			} else {
				dataPage := ReadDataPage(tree, currentIndexPage.Children[index])
				return dataPage
			}
		} else {
			// currentPageOffset = currentIndexPage.Children[index]
			if found {
				currentPageOffset = currentIndexPage.Children[index+1]
			} else {
				currentPageOffset = currentIndexPage.Children[index]
			}
		}
	}
}

func (tree *BTree[TKey, TValue]) insertToLeafNode(dataPage *DataPage[TKey, TValue], key TKey, value TValue) (int, bool /*isOverflowing*/) {
	_, shouldBeAt, alreadyExists := dataPage.findAndUpdateIfExists(key, value)

	if alreadyExists {
		// TODO Handle Updated
		return shouldBeAt, false
	} else {
		if dataPage.isOverflowing() {
			return shouldBeAt, true
		} else {
			dataPage.insertAt(shouldBeAt, key, value)
			SaveDataPage(tree, dataPage, dataPage.Offset)
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
		parentIndexPage = newIndexPage(tree)
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
	SaveIndexPage(tree, parent, parent.Offset)

	// Set parent for the new page
	newDataPage.Parent = parent.Offset

	newDataPage.Next = dataPage.Next
	if newDataPage.Next != -1 {
		nextDataPage := ReadDataPage(tree, newDataPage.Next)
		nextDataPage.Previous = newDataPage.Offset
		SaveDataPage(tree, nextDataPage, nextDataPage.Offset)
	}

	dataPage.Next = newDataPage.Offset
	newDataPage.Previous = dataPage.Offset
	SaveDataPage(tree, dataPage, dataPage.Offset)
	SaveDataPage(tree, newDataPage, newDataPage.Offset)

	currentParent := parent
	for currentParent != nil {
		if currentParent.isOverflowing() {
			currentParent = tree.splitAndPushIndexPage(currentParent)
		} else {
			break
		}
	}

	return parent
}

func (tree *BTree[TKey, TValue]) readRelationsOfIndexPage(indexPage *IndexPage[TKey, TValue]) (
	*IndexPage[TKey, TValue], *IndexPage[TKey, TValue], *IndexPage[TKey, TValue]) {
	if indexPage.Parent == -1 {
		// Root node has no parent.
		return nil, nil, nil
	}

	var parentIndexPage = ReadIndexPage(tree, indexPage.Parent)
	var leftIndexPage *IndexPage[TKey, TValue] = nil
	var rightIndexPage *IndexPage[TKey, TValue] = nil

	if indexPage.Previous != -1 {
		leftIndexPage = ReadIndexPage(tree, indexPage.Previous)
		if leftIndexPage.Parent != indexPage.Parent {
			leftIndexPage = nil
		}
	}

	if indexPage.Next != -1 {
		rightIndexPage = ReadIndexPage(tree, indexPage.Next)
		if rightIndexPage.Parent != indexPage.Parent {
			rightIndexPage = nil
		}
	}

	return parentIndexPage, leftIndexPage, rightIndexPage
}

func (tree *BTree[TKey, TValue]) readRelationsOfLeafPage(dataPage *DataPage[TKey, TValue]) (
	*IndexPage[TKey, TValue], *DataPage[TKey, TValue], *DataPage[TKey, TValue]) {
	var parentIndexPage *IndexPage[TKey, TValue] = ReadIndexPage((*BTree[TKey, TValue])(tree), dataPage.Parent)
	var leftDataPage *DataPage[TKey, TValue] = nil
	var rightDataPage *DataPage[TKey, TValue] = nil

	if dataPage.Previous != -1 {
		leftDataPage = ReadDataPage(tree, dataPage.Previous)
		if leftDataPage.Parent != dataPage.Parent {
			leftDataPage = nil
		}
	}

	if dataPage.Next != -1 {
		rightDataPage = ReadDataPage(tree, dataPage.Next)
		if rightDataPage.Parent != dataPage.Parent {
			rightDataPage = nil
		}
	}

	return parentIndexPage, leftDataPage, rightDataPage
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
	tree.Count++
	SaveMetadata(tree)
}

func (tree *BTree[TKey, TValue]) Get(key TKey) (*TValue, bool) {
	dataPage := tree.findDataPageFromIndexRoot(key)
	dataNodeIndex, found := binarySearchPage[TKey, TValue](dataPage.Container, key)

	if found {
		return &dataPage.Container[dataNodeIndex].Value, true
	}
	return nil, false
}

func (tree *BTree[TKey, TValue]) redistributeIndexPages(leftPage, rightPage *IndexPage[TKey, TValue], parent *IndexPage[TKey, TValue], isLeftDonor bool) {
	var keysToMove int
	if isLeftDonor {
		// Calculate the number of keys to move from left to right to balance the pages
		keysToMove = (leftPage.Count - rightPage.Count) / 2
		// Move keys and children from the end of leftPage to the beginning of rightPage
		for i := 0; i < keysToMove; i++ {
			rightPage.insertAt(0, leftPage.Container[leftPage.Count-keysToMove+i].Key)
			rightPage.insertChildAt(0, leftPage.Children[leftPage.Count-keysToMove+i])
			leftPage.deleteAtIndexAndSort(leftPage.Count - keysToMove + i)
			leftPage.deleteChildAt(leftPage.Count - keysToMove + i)
		}
		// Update the parent's key value that points to the right page
		parentKeyIndex, _ := binarySearchPage[TKey, TValue](parent.Container, rightPage.Container[0].Key)
		parent.Container[parentKeyIndex].Key = rightPage.Container[0].Key
	} else {
		// Calculate the number of keys to move from right to left to balance the pages
		keysToMove = (rightPage.Count - leftPage.Count) / 2
		// Move keys and children from the beginning of rightPage to the end of leftPage
		for i := 0; i < keysToMove; i++ {
			leftPage.insertAt(leftPage.Count, rightPage.Container[i].Key)
			leftPage.insertChildAt(leftPage.Count, rightPage.Children[i])
			rightPage.deleteAtIndexAndSort(i)
			rightPage.deleteChildAt(i)
		}
		// Update the parent's key value that points to the right page
		parentKeyIndex, _ := binarySearchPage[TKey, TValue](parent.Container, rightPage.Container[0].Key)
		if parentKeyIndex != -1 && parentKeyIndex < parent.Count+1 {
			parent.Container[parentKeyIndex-1].Key = rightPage.Container[0].Key
		}
	}

	// Save changes to both index pages and the parent index page
	SaveIndexPage(tree, leftPage, leftPage.Offset)
	SaveIndexPage(tree, rightPage, rightPage.Offset)
	SaveIndexPage(tree, parent, parent.Offset)
}

func (tree *BTree[TKey, TValue]) redistributeIndexPagesFromLeft(leftPage, rightPage *IndexPage[TKey, TValue], parent *IndexPage[TKey, TValue]) {
	// Move the parent key to the leftPage first
	parentKeyIndex, _ := binarySearchPage[TKey, TValue](parent.Container, leftPage.Container[leftPage.Count-1].Key)

	copy(rightPage.Container[1:], rightPage.Container[:])
	rightPage.Container[0] = parent.Container[parentKeyIndex]

	copy(rightPage.Children[1:], rightPage.Children[:])
	rightPage.Children[0] = leftPage.Children[leftPage.Count]
	tree.updateChildren(rightPage, leftPage, leftPage.Count)
	rightPage.Count++

	parent.Container[parentKeyIndex] = leftPage.Container[leftPage.Count-1]

	leftPage.deleteChildAt(leftPage.Count)
	leftPage.deleteAt(leftPage.Count - 1)

	// Persist changes
	SaveIndexPage(tree, leftPage, leftPage.Offset)
	SaveIndexPage(tree, rightPage, rightPage.Offset)
	SaveIndexPage(tree, parent, parent.Offset)
}

func (tree *BTree[TKey, TValue]) redistributeIndexPagesFromRight(leftPage, rightPage *IndexPage[TKey, TValue], parent *IndexPage[TKey, TValue]) {
	// Move the parent key to the leftPage first
	parentKeyIndex, _ := binarySearchPage[TKey, TValue](parent.Container, rightPage.Container[0].Key)
	if parentKeyIndex > 0 {
		leftPage.Container[leftPage.Count] = parent.Container[parentKeyIndex-1]
		leftPage.Children[leftPage.Count+1] = rightPage.Children[0]
		tree.updateChildren(leftPage, rightPage, 0)
		leftPage.Count++

		parent.Container[parentKeyIndex-1] = rightPage.Container[0]

		rightPage.deleteAtIndexAndSort(0)
		copy(rightPage.Children[:], rightPage.Children[1:])
	}

	// Persist changes
	SaveIndexPage(tree, leftPage, leftPage.Offset)
	SaveIndexPage(tree, rightPage, rightPage.Offset)
	SaveIndexPage(tree, parent, parent.Offset)
}

func (tree *BTree[TKey, TValue]) updateChildren(toParentPage, fromParentPage *IndexPage[TKey, TValue], childIndex int) {
	if fromParentPage.IsChildrenDataPage {
		childDataPage := ReadDataPage(tree, fromParentPage.Children[childIndex])
		childDataPage.Parent = toParentPage.Offset
		SaveDataPage(tree, childDataPage, childDataPage.Offset)
	} else {
		childIndexPage := ReadIndexPage(tree, fromParentPage.Children[childIndex])
		childIndexPage.Parent = toParentPage.Offset
		SaveIndexPage(tree, childIndexPage, childIndexPage.Offset)
	}
}

func (tree *BTree[TKey, TValue]) updateParentAfterMerge(parentPage *IndexPage[TKey, TValue], childKey TKey, isFromLeft bool) TKey {
	var keyIndex int
	keyIndex, _ = binarySearchPage[TKey, TValue](parentPage.Container, childKey)
	if !isFromLeft {
		keyIndex--
	}

	borrowedKey := parentPage.Container[keyIndex].Key
	copy(parentPage.Container[keyIndex:], parentPage.Container[keyIndex+1:])
	copy(parentPage.Children[keyIndex+1:], parentPage.Children[keyIndex+2:])
	parentPage.Count--

	// Save the updated parent page
	SaveIndexPage(tree, parentPage, parentPage.Offset)

	return borrowedKey
}

func (tree *BTree[TKey, TValue]) mergeIndexPages(leftPage, rightPage *IndexPage[TKey, TValue], borrowKey TKey) {
	leftPage.Container[leftPage.Count] = newIndexNode(borrowKey)
	leftPage.Count++

	// Merge internal nodes
	for i := 0; i < rightPage.Count; i++ {
		leftPage.Container[leftPage.Count] = rightPage.Container[i]
		leftPage.Children[leftPage.Count] = rightPage.Children[i]
		tree.updateChildren(leftPage, rightPage, i)
		leftPage.Count++
	}

	// Last child pointer
	if rightPage.Children[rightPage.Count] != -1 {
		leftPage.Children[leftPage.Count] = rightPage.Children[rightPage.Count]
		tree.updateChildren(leftPage, rightPage, rightPage.Count)
	}

	if rightPage.Next != -1 {
		nextRightPage := ReadIndexPage(tree, rightPage.Next)
		nextRightPage.Previous = leftPage.Offset
		leftPage.Next = rightPage.Next
		SaveIndexPage(tree, nextRightPage, nextRightPage.Offset)
	} else {
		leftPage.Next = -1 // Right page was the last one
	}
	SaveIndexPage(tree, leftPage, leftPage.Offset)
	//TODO  Free right page
}

func (tree *BTree[TKey, TValue]) handleIndexPageUnderflow(indexPage *IndexPage[TKey, TValue]) {
	parent, leftSibling, rightSibling := tree.readRelationsOfIndexPage(indexPage)

	// If the index page is the root and has only one child, make the child the new root
	if parent == nil {
		if indexPage.Count == 0 {
			if indexPage.IsChildrenDataPage {
				tree.RootOffset = indexPage.Children[0]
				tree.IsLeaf = true
			} else {
				childIndexPage := ReadIndexPage(tree, indexPage.Children[0])
				tree.RootOffset = childIndexPage.Offset
				tree.IsLeaf = false
			}
		}
		return
	}

	if leftSibling != nil && leftSibling.isLendable() {
		tree.redistributeIndexPagesFromLeft(leftSibling, indexPage, parent)
	} else if rightSibling != nil && rightSibling.isLendable() {
		tree.redistributeIndexPagesFromRight(indexPage, rightSibling, parent)
	} else if leftSibling != nil {
		key := tree.updateParentAfterMerge(parent, leftSibling.Container[0].Key, true)
		tree.mergeIndexPages(leftSibling, indexPage, key)
	} else if rightSibling != nil {
		key := tree.updateParentAfterMerge(parent, rightSibling.Container[0].Key, false)
		tree.mergeIndexPages(indexPage, rightSibling, key)
	}

	// If the parent is deficient after removal, handle the deficiency
	if parent.isDeficient() {
		tree.handleIndexPageUnderflow(parent)
	}
}

func printPage[TKey cmp.Ordered, TValue any, TTPage TPage[TKey, TValue]](page TTPage) {
	if page == nil {
		return
	}
	switch p := any(page).(type) {
	case *DataPage[TKey, TValue]:
		for i := 0; i < p.Count; i++ {
			log.Printf("%v ", p.Container[i])
		}
	case *IndexPage[TKey, TValue]:
		for i := 0; i < p.Count; i++ {
			log.Printf("%v ", p.Container[i])
		}
	}
}

func (tree *BTree[TKey, TValue]) handleUnderflow(dataPage *DataPage[TKey, TValue], key TKey) {
	parent, leftSibling, rightSibling := tree.readRelationsOfLeafPage(dataPage)

	if leftSibling != nil && leftSibling.isLendable() {
		tree.redistributeLeafPagesFromLeft(leftSibling, dataPage, parent)
	} else if rightSibling != nil && rightSibling.isLendable() {
		tree.redistributeLeafPagesFromRight(dataPage, rightSibling, parent)
	} else if leftSibling != nil {
		tree.mergeLeafPages(leftSibling, dataPage, parent)
	} else if rightSibling != nil {
		tree.mergeLeafPages(dataPage, rightSibling, parent)
	}
}

func (tree *BTree[TKey, TValue]) redistributeLeafPagesFromLeft(leftPage, rightPage *DataPage[TKey, TValue], parent *IndexPage[TKey, TValue]) {

	// Step 2: Move keys from left to right
	// Shift existing keys in rightPage to make room
	copy(rightPage.Container[1:], rightPage.Container[:rightPage.Count])

	rightPage.Container[0] = leftPage.Container[leftPage.Count-1]
	leftPage.deleteAt(leftPage.Count - 1)
	// Adjust counts
	rightPage.Count++

	// Step 3: Update parent key to reflect the new smallest key in the right page
	parentKeyIndex, _ := binarySearchPage[TKey, TValue](parent.Container, rightPage.Container[0].Key)
	parent.Container[parentKeyIndex].Key = rightPage.Container[0].Key

	// Step 4: Persist changes
	SaveDataPage(tree, leftPage, leftPage.Offset)
	SaveDataPage(tree, rightPage, rightPage.Offset)
	SaveIndexPage(tree, parent, parent.Offset)
}

func (tree *BTree[TKey, TValue]) redistributeLeafPagesFromRight(leftPage, rightPage *DataPage[TKey, TValue], parent *IndexPage[TKey, TValue]) {
	leftPage.Container[leftPage.Count] = rightPage.Container[0]
	// Adjust counts
	leftPage.Count++
	rightPage.Count--
	// Shift keys in rightPage to remove the moved keys
	copy(rightPage.Container[:], rightPage.Container[1:])

	// Step 3: Update parent key to reflect the new smallest key in the right page
	parentKeyIndex, _ := binarySearchPage[TKey, TValue](parent.Container, rightPage.Container[0].Key)
	if parentKeyIndex > 0 {
		parent.Container[parentKeyIndex-1].Key = rightPage.Container[0].Key
	}

	// Step 4: Persist changes
	SaveDataPage(tree, leftPage, leftPage.Offset)
	SaveDataPage(tree, rightPage, rightPage.Offset)
	SaveIndexPage(tree, parent, parent.Offset)
}

func (tree *BTree[TKey, TValue]) mergeLeafPages(leftPage, rightPage *DataPage[TKey, TValue], parent *IndexPage[TKey, TValue]) {
	// Step 1: Merge contents
	for _, item := range rightPage.Container[:rightPage.Count] {
		leftPage.Container[leftPage.Count] = item
		leftPage.Count++
	}

	// Step 2: Update sibling links
	if rightPage.Next != -1 {
		nextRightPage := ReadDataPage(tree, rightPage.Next)
		nextRightPage.Previous = leftPage.Offset
		leftPage.Next = rightPage.Next
		SaveDataPage(tree, nextRightPage, nextRightPage.Offset)
	} else {
		leftPage.Next = -1 // Right page was the last one
	}

	// Step 3: Remove right page (Assuming a function for freeing page space exists)
	// TODO FreeDataPage(tree, rightPage.Offset)

	// Step 4: Update parent
	var parentKeyIndex int
	if rightPage.Count > 0 {
		parentKeyIndex, _ = binarySearchPage[TKey, TValue](parent.Container, rightPage.Container[0].Key)
	} else {
		// Find the key in the parent that points to the rightPage by using the leftPage
		parentKeyIndex, _ = binarySearchPage[TKey, TValue](parent.Container, leftPage.Container[0].Key)
		parentKeyIndex++ // Increment to get the index of the key that pointed to rightPage
	}

	copy(parent.Container[parentKeyIndex:], parent.Container[parentKeyIndex+1:])
	copy(parent.Children[parentKeyIndex+1:], parent.Children[parentKeyIndex+2:])
	parent.Count--

	// Step 5: Save changes
	SaveDataPage(tree, leftPage, leftPage.Offset)
	//SaveIndexPage(tree, parent, parent.Offset)

	if parent.isDeficient() {
		// Handle underflow for the parent index page
		tree.handleIndexPageUnderflow(parent)
	} else {
		SaveIndexPage(tree, parent, parent.Offset)
	}
}

func (tree *BTree[TKey, TValue]) updateIfPresentInInternalPage(dataPage *DataPage[TKey, TValue], key TKey, inOrderKey TKey) {
	currentPageOffset := dataPage.Parent

	for currentPageOffset != -1 {
		currentIndexPage := ReadIndexPage(tree, currentPageOffset)
		index, found := binarySearchPage[TKey, TValue](currentIndexPage.Container, key)

		if found {
			currentIndexPage.Container[index].Key = inOrderKey
			SaveIndexPage(tree, currentIndexPage, currentIndexPage.Offset)
		}
		currentPageOffset = currentIndexPage.Parent
	}
}

func (tree *BTree[TKey, TValue]) getInOrderSuccessor(dataNodeIndex int, page *DataPage[TKey, TValue]) *TKey {
	if dataNodeIndex < page.Count-1 {
		return &page.Container[dataNodeIndex+1].Key
	}

	if page.Next != -1 {
		nextPage := ReadDataPage(tree, page.Next)
		return &nextPage.Container[0].Key
	}
	return nil
}

func (tree *BTree[TKey, TValue]) updateNodeIfKeyPresentInInternalNode(dataNodeIndex int, key TKey, dataPage *DataPage[TKey, TValue]) {
	// Update Parents if needed
	inOrderKey := tree.getInOrderSuccessor(dataNodeIndex, dataPage)
	if inOrderKey != nil {
		tree.updateIfPresentInInternalPage(dataPage, key, *inOrderKey)
	}
}

func (tree *BTree[TKey, TValue]) deleteFromDataPageAndPropagate(dataNodeIndex int, key TKey, dataPage *DataPage[TKey, TValue]) {
	dataPage.deleteAtIndexAndSort(dataNodeIndex)
	if dataPage.Parent != -1 && dataPage.isDeficient() {
		tree.handleUnderflow(dataPage, key)
	} else {
		SaveDataPage(tree, dataPage, dataPage.Offset)
	}
}

func (tree *BTree[TKey, TValue]) Delete(key TKey) (ok bool) {
	if tree.Count == 0 {
		return false
	}

	dataPage := tree.findDataPageFromIndexRoot(key)
	dataNodeIndex, found := binarySearchPage[TKey, TValue](dataPage.Container, key)
	if !found {
		return false
	}

	// Update Parents if needed
	tree.updateNodeIfKeyPresentInInternalNode(dataNodeIndex, key, dataPage)

	// Delete from data page and propagate to index pages
	tree.deleteFromDataPageAndPropagate(dataNodeIndex, key, dataPage)

	// Update tree
	tree.Count--
	SaveMetadata(tree)
	return true
}
