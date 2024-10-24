package bptree

import (
	"fmt"
	"os"
)

func getMetaMountPoint() string {
	dataFilePath := os.Getenv("META_MOUNT_POINT")

	if dataFilePath != "" {
		return dataFilePath
	}

	return "./sieve/meta"
}

var (
	MetaDirectory  = getMetaMountPoint()
	IndexDirectory = MetaDirectory + "/index"
)

func IndexFile(collectionName string, fieldName string) string {
	return IndexDirectory + "/" + collectionName + "-" + fieldName + ".idx.sieve"
}

func SubIndexFile(collectionName string, fieldName string, key any) string {
	return fmt.Sprintf("%s/%s-%s-%v.idx.sieve", IndexDirectory, collectionName, fieldName, key)
}
