package main

import (
	"fmt"
	"log"
	"time"
	// "slices"
)

// "slices"

func main() {
	var tree *BTree[int, int] = New[int, int](1024)
	timer := time.Now()

	total := 1000000000
	// log.Println(fmt.Sprintf("%s asdfa %s", 1, 2))
	for i := 0; i < total; i++ {
		tree.Put(i, i+1)
	}

	log.Println(fmt.Sprintf("Inserted %d items in %s", total, time.Since(timer)))
}
