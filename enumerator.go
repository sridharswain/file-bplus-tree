package bptree

import (
	"bptree/btree"
	"log"
	"os"
)

type Enumerator struct {
	btreeEnumerator *btree.Enumerator[any, any]
	file            *os.File
}

func (enumerator *Enumerator) Next() (*any, *ResultSet) {
	if !enumerator.HasNext() {
		return nil, nil
	}

	//timer := time.Now()
	key, value := enumerator.btreeEnumerator.Next(enumerator.file)
	if key == nil || value == nil {
		log.Printf("nil")
	}
	//log.Printf("Computing next, time=%s", time.Since(timer))
	return key, &ResultSet{treeValue: value}
}

func (enumerator *Enumerator) Previous() (*any, *ResultSet) {
	if !enumerator.HasPrevious() {
		return nil, nil
	}

	key, value := enumerator.btreeEnumerator.Previous(enumerator.file)
	return key, &ResultSet{treeValue: value}
}

func (enumerator *Enumerator) HasNext() bool {
	return enumerator.btreeEnumerator.HasNext()
}

func (enumerator *Enumerator) HasPrevious() bool {
	return enumerator.btreeEnumerator.HasPrevious()
}

func (enumerator *Enumerator) Close() {
	enumerator.btreeEnumerator.Close()
	enumerator.file.Close()
}
