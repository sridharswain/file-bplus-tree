package main

import (
	"fmt"
	"log"
	// "slices"
)

// "slices"

func main() {
	// d := []int{1, 2, 5, 7, 9}

	// i, found := slices.BinarySearch(d, 6)

	// x := 1

	// switch x {
	// case 1:
	// case 2:
	// 	log.Println("2")
	// }

	// log.Println(i, found)

	var page *DataPage[int, int] = newDataPage[int, int](nil, nil)

	page.insert(1, 2, nil)
	page.insert(5, 6, nil)
	// page.insert(3, 4, nil)

	log.Println(fmt.Sprintf("%s", page.container))
}
