# B+ Tree Implementation

This project provides an implementation of a B+ Tree in Go, designed for efficient storage and retrieval of data. The B+ Tree is a self-balancing tree data structure that maintains sorted data and allows for efficient insertion, deletion, and search operations.

## Features

- **Efficient Data Storage**: Store and retrieve data with high performance.
- **Range Queries**: Perform range queries to fetch data within a specified range.
- **Concurrency**: Thread-safe operations with read-write locks.
- **Persistence**: Store tree data in a file for persistence.

## Benefits of Persistence

Persisting the B+ Tree index into a file offers several advantages:

- **Data Durability**: Ensures that the index is not lost when the application is restarted or crashes.
- **Faster Startup**: Allows the application to quickly load the existing index from the file, avoiding the need to rebuild the index from scratch.
- **Consistency**: Maintains a consistent state of the index across application runs.
- **Scalability**: Supports larger datasets by offloading storage to disk, reducing memory usage.

## Getting Started

### Prerequisites

- Go 1.16 or later

### Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/sridharswain/file-bplus-tree.git
    cd file-bplus-tree
    ```

2. Install dependencies:
    ```sh
    go mod tidy
    ```

### Usage

1. Create a new B+ Tree and insert data:
    ```go
    package main

    import (
        "bptree"
        "bptree/dbmodels"
        "fmt"
        "os"
    )

    func main() {
        // Create a new Tree
        tree := bptree.New("test_collection", "test_field")

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
    ```

2. Run the program:
    ```sh
    go run main.go
    ```

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Acknowledgments

- Inspired by various B+ Tree implementations and database indexing techniques.

---

Happy coding! 🚀