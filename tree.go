package bptree

import (
	"bptree/btree"
	"bptree/dbmodels"
	"bptree/utils"
	"os"
	"sync"
)

const (
	BTreeOrder                = 32
	SubBTreeOrder             = 16
	SubBTreeCreationThreshold = 16
)

type Tree struct {
	lock           sync.RWMutex
	index          *btree.BTree[any, any]
	indexFile      string
	collectionName string
	fieldName      string
}

func indexFileExists(file *os.File) bool {
	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}
	if fileInfo.Size() <= 0 {
		return false
	}
	return true
}

func newSubBtree(indexName string) (*btree.BTree[any, *dbmodels.Page], *os.File) {
	file, err := os.OpenFile(indexName, os.O_CREATE|os.O_RDWR, os.ModePerm)

	if err != nil {
		panic(err)
	}

	var tree *btree.BTree[any, *dbmodels.Page]

	if indexFileExists(file) {
		tree = btree.ReadMetadata[any, *dbmodels.Page](file)
	} else {
		tree = btree.NewTree[any, *dbmodels.Page](indexName, SubBTreeOrder, file)
	}

	return tree, file

}

func New(collectionName string, fieldName string) *Tree {
	indexName := IndexFile(collectionName, fieldName)
	file, err := os.OpenFile(indexName, os.O_CREATE|os.O_RDWR, os.ModePerm)

	if err != nil {
		panic(err)
	}

	var tree *btree.BTree[any, any]

	if indexFileExists(file) {
		tree = btree.ReadMetadata[any, any](file)
	} else {
		tree = btree.NewTree[any, any](indexName, BTreeOrder, file)
	}

	defer file.Close()
	return &Tree{
		index:          tree,
		collectionName: collectionName,
		fieldName:      fieldName,
		indexFile:      indexName,
	}
}

func (tree *Tree) Put(primaryKeyValue any, key any, page *dbmodels.Page) {
	file, err := os.OpenFile(tree.indexFile, os.O_RDWR, os.ModePerm)

	if err != nil {
		panic(err)
	}
	defer file.Close()

	tree.lock.Lock()
	defer tree.lock.Unlock()

	if existingData, exists := tree.index.Get(key, file); exists {
		dataIndex := *existingData
		tree.resolveBtreeValueAndPut(primaryKeyValue, key, page, dataIndex, file)
	} else {
		tree.index.Put(key, map[any]*dbmodels.Page{primaryKeyValue: page}, file)
	}
}

func (tree *Tree) resolveBtreeValueAndPut(primaryKeyValue any, key any, page *dbmodels.Page,
	value any, file *os.File) {
	switch existingValue := value.(type) {
	case map[any]*dbmodels.Page:
		if primaryKeyValue == key && len(existingValue) < SubBTreeCreationThreshold-1 {
			existingValue[primaryKeyValue] = page
			tree.index.Put(key, existingValue, file)
		} else {
			subBTree, subFile := newSubBtree(SubIndexFile(tree.collectionName, tree.fieldName, key))
			for primaryKey, location := range existingValue {
				subBTree.Put(primaryKey, location, subFile)
			}
			subBTree.Put(primaryKeyValue, page, subFile)
			subFile.Close()

			tree.index.Put(key, subBTree, file)
		}
	case btree.BTree[any, *dbmodels.Page]:
		subTreeFile, err := os.OpenFile(existingValue.IndexName, os.O_RDWR, os.ModePerm)
		if err != nil {
			panic(err)
		}
		existingValue.Put(primaryKeyValue, page, subTreeFile)
		subTreeFile.Close()
		tree.index.Put(key, existingValue, file)
	}
}

func (tree *Tree) Get(key any) (*map[any]*dbmodels.Page, bool) {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	file, err := os.OpenFile(tree.indexFile, os.O_RDWR, os.ModePerm)

	if err != nil {
		panic(err)
	}
	defer file.Close()

	existingData, exists := tree.index.Get(key, file)
	if !exists {
		return nil, false
	}

	switch value := (*existingData).(type) {
	case map[any]*dbmodels.Page:
		return &value, true
	case btree.BTree[any, *dbmodels.Page]:
		subTreeFile, err := os.OpenFile(value.IndexName, os.O_RDWR, os.ModePerm)
		if err != nil {
			panic(err)
		}
		defer subTreeFile.Close()
		e := value.SeekFirst(subTreeFile)
		dataMap := map[any]*dbmodels.Page{}
		for e.HasNext() {
			k, v := e.Next(subTreeFile)
			dataMap[*k] = *v
		}
		return &dataMap, true
	default:
		return nil, false
	}
}

func (tree *Tree) SeekFirst() *Enumerator {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	file, err := os.OpenFile(tree.indexFile, os.O_RDWR, os.ModePerm)

	if err != nil {
		panic(err)
	}

	return &Enumerator{btreeEnumerator: tree.index.SeekFirst(file), file: file}
}

func (tree *Tree) Seek(key any) *Enumerator {
	tree.lock.RLock()
	defer tree.lock.RUnlock()
	file, err := os.OpenFile(tree.indexFile, os.O_RDWR, os.ModePerm)

	if err != nil {
		panic(err)
	}

	return &Enumerator{btreeEnumerator: tree.index.Seek(key, file), file: file}
}

func (tree *Tree) SeekLast() *Enumerator {
	tree.lock.RLock()
	defer tree.lock.RUnlock()
	file, err := os.OpenFile(tree.indexFile, os.O_RDWR, os.ModePerm)

	if err != nil {
		panic(err)
	}

	return &Enumerator{btreeEnumerator: tree.index.SeekLast(file), file: file}
}

func (tree *Tree) Count() int {
	return tree.index.Count
}

// In Gets values from index of keys passed in array. when passed in sorted order
func (tree *Tree) In(keys []any) map[any]*dbmodels.Page {
	if len(keys) == 0 {
		return map[any]*dbmodels.Page{}
	}

	tree.lock.RLock()
	defer tree.lock.RUnlock()

	var result = map[any]*dbmodels.Page{} //Result container

	for _, key := range keys {
		if val, exists := tree.Get(key); exists {
			for primaryKey, location := range *val {
				result[primaryKey] = location
			}
		}
	}

	return result
}

// In Gets values from index of keys passed in array. when passed in sorted order
func (tree *Tree) InSorted(keys []any, limit int, seek int) []*dbmodels.PrimaryKeyPageTuple {
	if len(keys) == 0 {
		return []*dbmodels.PrimaryKeyPageTuple{}
	}

	tree.lock.RLock()
	defer tree.lock.RUnlock()

	result := make([]*dbmodels.PrimaryKeyPageTuple, 0) //Result container
	var i int
	var resultCount = 0

inIndexWalk:
	for _, key := range keys {
		if val, exists := tree.Get(key); exists {
			for primaryKey, location := range *val {
				if resultCount < seek {
					resultCount++
				} else {
					if i < limit {
						result = append(result, &dbmodels.PrimaryKeyPageTuple{PrimaryKey: primaryKey, Page: location, Key: key})
						i++
					} else {
						break inIndexWalk
					}
				}
			}
		}
	}

	return result
}

func (tree *Tree) InKeysOf(keys []any) []*dbmodels.Page {
	if len(keys) == 0 {
		return []*dbmodels.Page{}
	}

	tree.lock.RLock()
	defer tree.lock.RUnlock()

	var result []*dbmodels.Page //Result container

	for _, key := range keys {
		val, exists := tree.Get(key)
		if exists {
			for _, location := range *val {
				result = append(result, location)
			}
		}
	}

	return result
}

// Gets values from index of keys passed in array. when passed in sorted order
func (tree *Tree) InAndRelevantKeys(keys []any, relevantKeys map[any]float64) map[any]*dbmodels.Page {
	if len(relevantKeys) == 0 {
		return tree.In(keys)
	}

	tree.lock.RLock()
	defer tree.lock.RUnlock()

	var result = map[any]*dbmodels.Page{} //Result container

	for _, key := range keys {
		val, exists := tree.Get(key)
		if exists {
			for primaryKey := range relevantKeys {
				if location, existsInKeys := (*val)[primaryKey]; existsInKeys {
					result[primaryKey] = location
				}
			}
		}
	}
	return result
}

// Gets values from index of keys passed in array. when passed in sorted order
func (tree *Tree) InAndRelevantKeysSorted(keys []any, relevantKeys map[any]float64, limit int, seek int) []*dbmodels.PrimaryKeyPageTuple {
	if len(relevantKeys) == 0 {
		return tree.InSorted(keys, limit, seek)
	}

	tree.lock.RLock()
	defer tree.lock.RUnlock()

	result := make([]*dbmodels.PrimaryKeyPageTuple, 0) //Result container
	var i int
	var resultCount = 0

inAndRelevantKeyWalk:
	for _, key := range keys {
		val, exists := tree.Get(key)
		if exists {
			for primaryKey := range relevantKeys {
				if location, existsInKeys := (*val)[primaryKey]; existsInKeys {
					if resultCount < seek {
						resultCount++
					} else {
						if i < limit {
							result = append(result, &dbmodels.PrimaryKeyPageTuple{PrimaryKey: primaryKey, Page: location, Key: key})
							i++
						} else {
							break inAndRelevantKeyWalk
						}
					}
				}
			}
		}
	}
	return result
}

func (tree *Tree) Range(lower any, upper any) map[any]*dbmodels.Page {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	// If lower is greater then return empty
	if utils.Compare(lower, upper) == 1 {
		return map[any]*dbmodels.Page{}
	}

	// If lower is out of bounds then return empty array
	e := tree.Seek(lower)

	var result = map[any]*dbmodels.Page{} //Result container
	for e.HasNext() {
		key, val := e.Next()
		if utils.Compare(key, upper) <= 0 {
			for primaryKey, location := range val.ToIterable() {
				result[primaryKey] = location
			}
		} else {
			break
		}
	}

	e.Close()
	return result
}

func (tree *Tree) RangeSorted(lower any, upper any, limit int, seek int) []*dbmodels.PrimaryKeyPageTuple {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	// If lower is greater then return empty
	if utils.Compare(lower, upper) == 1 {
		return []*dbmodels.PrimaryKeyPageTuple{}
	}

	// If lower is out of bounds then return empty array
	e := tree.Seek(lower)

	var i int
	var resultCount = 0
	var result = make([]*dbmodels.PrimaryKeyPageTuple, 0) //Result container

rangeSortedIndexWalk:
	for e.HasNext() {
		key, val := e.Next()
		if utils.Compare(key, upper) <= 0 {
			for primaryKey, location := range val.ToIterable() {
				if resultCount < seek {
					resultCount++
				} else {
					if i < limit {
						result = append(result, &dbmodels.PrimaryKeyPageTuple{PrimaryKey: primaryKey, Page: location, Key: key})
						i++
					} else {
						break rangeSortedIndexWalk
					}
				}
			}
		} else {
			break
		}
	}

	e.Close()
	return result
}

func (tree *Tree) RangeAndRelevantKeys(lower any, upper any, relevantKeys map[any]float64) map[any]*dbmodels.Page {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	if len(relevantKeys) == 0 {
		return tree.Range(lower, upper)
	}

	// If lower is greater then return empty
	if utils.Compare(lower, upper) == 1 {
		return map[any]*dbmodels.Page{}
	}

	// If lower is out of bounds then return empty array
	e := tree.Seek(lower)

	var result = map[any]*dbmodels.Page{} //Result container
	for e.HasNext() {
		key, val := e.Next()
		if utils.Compare(key, upper) <= 0 {
			for primaryKey := range relevantKeys {
				if location, existsInKeys := val.Has(primaryKey); existsInKeys {
					result[primaryKey] = location
				}
			}
		} else {
			break
		}
	}

	e.Close()
	return result
}

func (tree *Tree) RangeAndRelevantKeysSorted(lower any, upper any, relevantKeys map[any]float64, limit int, seek int) []*dbmodels.PrimaryKeyPageTuple {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	if len(relevantKeys) == 0 {
		return tree.RangeSorted(lower, upper, limit, seek)
	}

	// If lower is greater then return empty
	if utils.Compare(lower, upper) == 1 {
		return []*dbmodels.PrimaryKeyPageTuple{}
	}

	// If lower is out of bounds then return empty array
	e := tree.Seek(lower)

	var i int
	var resultCount = 0
	var result = make([]*dbmodels.PrimaryKeyPageTuple, 0) //Result container

rangeAndRelevantKeyWalk:
	for e.HasNext() {
		key, val := e.Next()
		if utils.Compare(key, upper) <= 0 {
			for primaryKey := range relevantKeys {
				if location, existsInKeys := val.Has(primaryKey); existsInKeys {
					if resultCount < seek {
						resultCount++
					} else {
						if i < limit {
							result = append(result, &dbmodels.PrimaryKeyPageTuple{PrimaryKey: primaryKey, Page: location, Key: key})
							i++
						} else {
							break rangeAndRelevantKeyWalk
						}
					}
				}
			}
		} else {
			break
		}
	}

	e.Close()
	return result
}

func (tree *Tree) All(limit, seek int) []*dbmodels.SortParamLocation {
	tree.lock.RLock()
	defer tree.lock.RUnlock()
	e := tree.SeekFirst()

	var i int
	var resultCount = 0
	result := make([]*dbmodels.SortParamLocation, 0) //Result container

indexWalk:
	for e.HasNext() {
		key, val := e.Next()
		for _, location := range val.ToIterable() {
			if resultCount < seek {
				resultCount++
			} else {
				if i < limit {
					result = tree.appendResultSorted(result, location, *key)
					i++
				} else {
					break indexWalk
				}
			}

		}
	}

	e.Close()
	return result
}

func (tree *Tree) AllReverse(limit, seek int) []*dbmodels.SortParamLocation {
	tree.lock.RLock()
	defer tree.lock.RUnlock()
	e := tree.SeekLast()

	var i int
	var resultCount = 0

	result := make([]*dbmodels.SortParamLocation, 0) //Result container

indexWalk:
	for e.HasPrevious() {
		key, val := e.Previous()
		for _, location := range val.ToIterable() {
			if resultCount < seek {
				resultCount++
			} else {
				if i < limit {
					result = tree.appendResultSorted(result, location, *key)
					i++
				} else {
					break indexWalk
				}
			}

		}
	}

	e.Close()
	return result
}

func (tree *Tree) appendResultSorted(result []*dbmodels.SortParamLocation, row *dbmodels.Page, key any) []*dbmodels.SortParamLocation {
	if len(result) == 0 {
		result = append(result, &dbmodels.SortParamLocation{
			SortParam: key,
			Locations: []*dbmodels.Page{row},
		})
	} else {
		if utils.Compare(result[len(result)-1].SortParam, key) == 0 {
			result[len(result)-1].Locations = append(result[len(result)-1].Locations, row)
		} else {
			result = append(result, &dbmodels.SortParamLocation{
				SortParam: key,
				Locations: []*dbmodels.Page{row},
			})
		}
	}
	return result
}
