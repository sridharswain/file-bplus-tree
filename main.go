package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"
)

func main() {
	tree := New[int, int]("example.tree", 128)

	timer := time.Now()

	// // Write
	x := 1000000
	for i := 0; i < x; i++ {
		rand := rand.Intn(x * 10)
		internalTime := time.Now()
		tree.Put(rand, i+1)
		log.Printf("Put %s in %s s", rand, time.Since(internalTime))
	}

	log.Println(fmt.Sprintf("Time taken to write %s data : %s", strconv.Itoa(x), time.Since(timer)))

	// Read
	y := x
	for i := 0; i < y; i++ {
		rand := rand.Intn(x * 10)
		internalTime := time.Now()
		value, found := tree.Get(rand)
		if found {
			log.Printf("Get %s, %s, %s in %s,", rand, found, strconv.Itoa(*value), time.Since(internalTime))
		} else {
			log.Printf("Not found, %s", rand)
		}
	}

	log.Println(fmt.Sprintf("Time taken to read %s data : %s", strconv.Itoa(y), time.Since(timer)))

	// value, found := tree.Get(998600)
	// if found {
	// 	log.Println(found, strconv.Itoa(*value))
	// } else {
	// 	log.Println("Not found")
	// }
}
