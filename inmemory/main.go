package main

import (
	"fmt"
	"log"
	"time"
	// "slices"
)

// "slices"

func main() {
	var tree *BTree[int, int] = New[int, int](3)
	timer := time.Now()

	total := 7
	// log.Println(fmt.Sprintf("%s asdfa %s", 1, 2))
	for i := 1; i < total + 1; i++ {
		tree.Put(i, i+1)
	}

	tree.Get(2)
	log.Println(fmt.Sprintf("Inserted %d items in %s", total, time.Since(timer)))
}
