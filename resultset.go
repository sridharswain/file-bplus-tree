package bptree

import (
	"bptree/btree"
	"bptree/dbmodels"
	"os"
)

type ResultSet struct {
	treeValue *any
}

func (row *ResultSet) Has(primaryKey any) (*dbmodels.Page, bool) {
	switch value := (*row.treeValue).(type) {
	case map[any]*dbmodels.Page:
		val, ok := value[primaryKey]
		return val, ok
	case btree.BTree[any, *dbmodels.Page]:
		subTreeFile, err := os.OpenFile(value.IndexName, os.O_RDWR, os.ModePerm)
		if err != nil {
			panic(err)
		}
		defer subTreeFile.Close()
		val, ok := value.Get(primaryKey, subTreeFile)
		if ok {
			return *val, ok
		}
		return nil, false
	default:
		return nil, false
	}
}

func (row *ResultSet) ToIterable() map[any]*dbmodels.Page {
	switch existingData := (*row.treeValue).(type) {
	case map[any]*dbmodels.Page:
		return existingData
	case btree.BTree[any, *dbmodels.Page]:
		subTreeFile, err := os.OpenFile(existingData.IndexName, os.O_RDWR, os.ModePerm)
		if err != nil {
			panic(err)
		}
		defer subTreeFile.Close()

		dataMap := map[any]*dbmodels.Page{}
		e := existingData.SeekFirst(subTreeFile)
		for e.HasNext() {
			k, v := e.Next(subTreeFile)
			dataMap[*k] = *v
		}
		return dataMap
	default:
		return nil
	}
}
