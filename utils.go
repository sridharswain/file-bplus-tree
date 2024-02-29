package main

import (
	"cmp"
	"slices"
)

func binarySearchPage[TKey cmp.Ordered, TValue any, TTNode TNode[TKey, TValue]](space []TTNode, key TKey) (int, bool) {
	return slices.BinarySearchFunc(space, key, func(t1 TTNode, t2 TKey) int {
		if t1 == nil {
			return +1
		}
		switch x := any(t1).(type) {
		case *DataNode[TKey, TValue]:
			return cmp.Compare(x.key, t2)
		case *IndexNode[TKey]:
			return cmp.Compare(x.key, t2)
		}
		return -1
	})
}