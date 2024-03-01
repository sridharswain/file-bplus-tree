package main

import (
	"fmt"
	"log"
	"time"
)

func main() {
	tree := New[int, int]("hello", 10)

	timer := time.Now()

	x:= 1000
	for i := 0; i < 10000; i++ {
		tree.Put(i, 100)
	}


	// tree.Put(1, 100)
	// tree.Put(2, 100)
	// tree.Put(3, 100)
	// tree.Put(4, 100)
	// tree.Put(5, 100)
	// tree.Put(6, 100)
	// // tree.Put(7, 100)
	// tree.Put(8, 100)
	// tree.Put(9, 100)
	// tree.Put(10, 100)
	log.Println(fmt.Sprintf("Taken taken to push %s data : %s", x, time.Since(timer)))
}
