package main

import (
	"fmt"
	"log"
	// "slices"
)

// "slices"

func main() {
	var page *DataPage[int, int] = newDataPage[int, int](nil, nil)

	page.insert(1, 2, nil)
	page.insert(5, 6, nil)
	page.insert(3, 4, nil)

	log.Println(fmt.Sprintf("%s", page.container))
}
