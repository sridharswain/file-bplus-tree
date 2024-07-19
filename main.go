package main

import (
	"math/rand"
	"time"
	// "math/rand"
	// "strconv"
	//"time"
)

func createRandomNaturals(size int, count int) []int {
	// Initialize the seed for random number generation
	rand.Seed(time.Now().UnixNano())

	// Create an array of the specified size filled with zeros
	arr := make([]int, size)

	// Ensure count does not exceed size
	if count > size {
		count = size
	}

	for i := 1; i <= count; i++ {
		for {
			// Generate a random index
			index := rand.Intn(size)

			// If the position at the index is zero, place the natural number there
			if arr[index] == 0 {
				arr[index] = i
				break // Move to the next number after placing the current one
			}
		}
	}

	return arr
}

func main() {

	rawLog, logFile := InitializeLogger()
	log := rawLog.Sugar()
	defer logFile.Close()

	indexName := "example.tree"
	//if _, err := os.Stat(indexName); err == nil {
	//	// file does exist
	//	err := os.Remove(indexName)
	//	if err != nil {
	//		return
	//	}
	//}
	arr := createRandomNaturals(10000000, 10000000)
	tree := New[int, int](indexName, 128)

	// timer := time.Now()

	// // // Write
	// x := 10000000
	// for i := 0; i < x; i++ {
	// 	// rand := rand.Intn(x * 10)
	// 	internalTime := time.Now()
	// 	tree.Put(i, i+1)
	// 	log.Printf("Put %s in %s s", i, time.Since(internalTime))
	// }

	// log.Println(fmt.Sprintf("Time taken to write %s data : %s", strconv.Itoa(x), time.Since(timer)))

	//for i := 1; i <= 10; i++ {
	//	tree.Put(i, 1)
	//}

	d := 1000000
	writeTimer := time.Now()

	for _, value := range arr {
		singleWriteTimer := time.Now()
		tree.Put(value, 1)
		log.Infof("action=put, tree_size=%d, time=%d", tree.Count, time.Since(singleWriteTimer))
	}

	log.Infof("action=all_put, tree_size=%d time=%d", tree.Count, time.Since(writeTimer))

	readTimer := time.Now()
	for _, value := range arr {
		singleReadTimer := time.Now()
		//log.Printf("Deleting %d", x)
		//tree.Delete(x)
		_, found := tree.Get(value)
		log.Infof("action=get, tree_size=%d, key=%d, found=%t time=%d", tree.Count, value, found, time.Since(singleReadTimer))
	}
	log.Infof("action=all_get, tree_size=%d time=%d", tree.Count, time.Since(readTimer))

	delTimer := time.Now()

	tree.Delete(1)
	tree.Delete(2)
	tree.Delete(3)
	tree.Delete(4)
	tree.Delete(5)
	tree.Delete(6)
	tree.Delete(10)
	tree.Delete(11)
	tree.Delete(13)
	tree.Delete(12)
	tree.Delete(14)
	tree.Delete(15)
	tree.Delete(16)
	tree.Delete(17)
	tree.Delete(18)
	tree.Delete(19)
	tree.Delete(20)
	tree.Delete(40)
	tree.Delete(31)
	tree.Delete(35)
	tree.Delete(33)
	tree.Delete(32)
	tree.Delete(34)
	tree.Delete(42)
	tree.Delete(39)
	tree.Delete(44)
	tree.Delete(37)
	tree.Delete(43)
	tree.Delete(45)
	tree.Delete(36)
	tree.Delete(38)
	tree.Delete(41)
	tree.Delete(30)
	tree.Delete(29)
	tree.Delete(28)
	tree.Delete(27)
	tree.Delete(26)
	tree.Delete(25)
	tree.Delete(50)
	tree.Delete(49)
	tree.Delete(48)
	tree.Delete(47)
	tree.Delete(46)
	tree.Delete(45)
	tree.Delete(1000)
	tree.Delete(1031)
	tree.Delete(10210)
	tree.Delete(100123)

	//time.Sleep(10 * time.Millisecond)
	log.Infof("action=delete, tree_size=%d time=%d", d, time.Since(delTimer))

	readTimer = time.Now()
	for _, value := range arr {
		singleReadTimer := time.Now()
		_, found := tree.Get(value)
		log.Infof("action=get_after_delete, tree_size=%d, key=%d, found=%t time=%d", tree.Count, value, found, time.Since(singleReadTimer))
	}
	log.Infof("action=get_with_delete, tree_size=%d time=%d", tree.Count, time.Since(readTimer))

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
