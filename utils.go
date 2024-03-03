package main

import (
	"cmp"
	"slices"
)

func binarySearchPage[TKey cmp.Ordered, TValue any, TTNode TNode[TKey, TValue]](space []TTNode, key TKey) (int, bool) {
	return slices.BinarySearchFunc(space, key, func(t1 TTNode, t2 TKey) int {
		switch x := any(t1).(type) {
		case DataNode[TKey, TValue]:
			if !x.Exists {
				return +1
			}
			return cmp.Compare(x.Key, t2)
		case IndexNode[TKey]:
			if !x.Exists {
				return +1
			}
			return cmp.Compare(x.Key, t2)
		}
		return -1
	})
}
