package main

import (
	"fmt"
	"log"
	// "slices"
)

// "slices"

func main() {
	var tree *BTree[int, int] = New[int, int](3)

	tree.Put(1, 100)
	tree.Put(2, 99)
	tree.Put(3, 98)
	tree.Put(4, 97)

	log.Println(fmt.Sprintf("%s", tree.root))
}
