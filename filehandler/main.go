package main

import (
	"log"
	"time"
)

func main() {
	tree := New[int, int]("hello", 128)

	timer := time.Now()
	for i := 0; i < 150; i++ {
		tree.Put(i, 100)
	}
	log.Printf("Taken taken %s", time.Since(timer))
}
