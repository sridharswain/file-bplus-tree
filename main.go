package main

import (
	// "fmt"
	"log"
	"os"
	// "math/rand"
	// "strconv"
	// "time"
)

func main() {
	indexName := "example.tree"
	if _, err := os.Stat(indexName); err == nil {
		// file does not exist
		os.Remove(indexName)
	}
	tree := New[int, int](indexName, 3)

	// timer := time.Now()

	// // // Write
	// x := 2
	// for i := 0; i < x; i++ {
	// 	// rand := rand.Intn(x * 10)
	// 	internalTime := time.Now()
	// 	tree.Put(i, i+1)
	// 	log.Printf("Put %s in %s s", i, time.Since(internalTime))
	// }

	// log.Println(fmt.Sprintf("Time taken to write %s data : %s", strconv.Itoa(x), time.Since(timer)))

	tree.Put(1, 1)
	tree.Put(2, 1)
	tree.Put(3, 1)
	tree.Put(4, 1)
	tree.Put(5, 1)
	tree.Put(6, 1)
	tree.Put(7, 1)

	tree.Delete(6)

	value, found := tree.Get(7)
	log.Printf("result, %s: %s", value, found)

	value, found = tree.Get(6)
	log.Printf("result, %s: %s", value, found)

	// Read
	// y := x
	// for i := 0; i < y; i++ {
	// 	rand := rand.Intn(x * 10)
	// 	internalTime := time.Now()
	// 	value, found := tree.Get(rand)
	// 	if found {
	// 		log.Printf("Get %s, %s, %s in %s,", rand, found, strconv.Itoa(*value), time.Since(internalTime))
	// 	} else {
	// 		log.Printf("Not found, %s", rand)
	// 	}
	// }

	// log.Println(fmt.Sprintf("Time taken to read %s data : %s", strconv.Itoa(y), time.Since(timer)))

	// value, found := tree.Get(998600)
	// if found {
	// 	log.Println(found, strconv.Itoa(*value))
	// } else {
	// 	log.Println("Not found")
	// }
}
