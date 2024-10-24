package bptree

import (
	"bptree/dbmodels"
	"fmt"
	"os"
)

func main() {
	// Create a new Tree
	tree := New("test_collection", "test_field")

	// Open a file to store the Tree data
	file, err := os.OpenFile("btree_data.dat", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Insert some data into the Tree
	for i := 1; i <= 10; i++ {
		page := &dbmodels.Page{DataOffset: int64(i), FileOffset: uint8(i)}
		tree.Put(i, i, page)
	}

	// Search for a key in the Tree
	keyToSearch := 5
	pageMap, found := tree.Get(keyToSearch)
	if found {
		fmt.Printf("Found key %d: %+v\n", keyToSearch, pageMap)
	} else {
		fmt.Printf("Key %d not found\n", keyToSearch)
	}

	// Iterate over the Tree
	enumerator := tree.SeekFirst()
	for enumerator.HasNext() {
		k, v := enumerator.Next()
		fmt.Printf("Key: %v, Value: %+v\n", k, v)
	}
}
