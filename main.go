package main

import (
	"fmt"
	"log"
	"strconv"
	"time"
)

func main() {
	tree := New[int, int]("hello", 128)

	timer := time.Now()

	x:= 100000
	for i := 0; i < x; i++ {
		tree.Put(i, 100)
	}


	// tree.Put(1, 100)
	// tree.Put(2, 100)
	// tree.Put(3, 100)
	// tree.Put(4, 100)
	// tree.Put(5, 100)
	// tree.Put(6, 100)
	// tree.Put(7, 100)
	// tree.Put(8, 100)
	// tree.Put(9, 100)
	// tree.Put(10, 100)
	log.Println(fmt.Sprintf("Taken taken to push %s data : %s", strconv.Itoa(x), time.Since(timer)))
}
