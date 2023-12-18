# enchanted-sleeve

[![Run Tests](https://github.com/yeqown/enchanted-sleeve/actions/workflows/go.yml/badge.svg)](https://github.com/yeqown/enchanted-sleeve/actions/workflows/go.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/yeqown/enchanted-sleeve.svg)](https://pkg.go.dev/github.com/yeqown/enchanted-sleeve)
[![Go Report Card](https://goreportcard.com/badge/github.com/yeqown/enchanted-sleeve)](https://goreportcard.com/report/github.com/yeqown/enchanted-sleeve)
[![codecov](https://codecov.io/gh/yeqown/enchanted-sleeve/branch/main/graph/badge.svg?token=t9YGWLh05g)](https://codecov.io/gh/yeqown/enchanted-sleeve)

enchanted-sleeve is a key-value (KV) storage engine based on Log-Structured Merge-tree (LSM). It is designed for efficient storage and retrieval of key-value pairs with high write throughput. It is a simple
key-value store that supports basic operations like `get`, `put`, `delete`, and
`list`.

> Enchanted sleeves is from the chinese myth item which called "蟒袖" that can 
> store things in it and what the most important is that it store things 
> more than its size, even a mountain.

### Installation

Before installing enchanted-sleeve, make sure you have Go installed on your system. You can download Go from [here](https://golang.org/dl/).

Once you have Go installed, you can clone and install enchanted-sleeve with the following commands:

```
git clone https://github.com/yeqown/enchanted-sleeve.git
cd enchanted-sleeve
go build
```

This will build the enchanted-sleeve executable in the current directory.

### Getting started

### Usage

The repository's key methods include `Open`, `Put`, `Get`, `Delete`, and `Close`. Below is a brief look at how to use them to create and manipulate a key-value store.

```go
// Create a new database instance with default options
db, err := esl.Open("path/to/directory")
if err != nil {
    log.Fatalf("failed to open database: %v", err)
}

defer db.Close()

// Set key-value pairs into the database
err = db.Put([]byte("key"), []byte("value"))
if err != nil {
    log.Fatalf("failed to put data: %v", err)
}

// Retrieve the value associated with the key
value, err := db.Get([]byte("key"))
if err != nil {
    log.Fatalf("failed to get data: %v", err)
}
fmt.Printf("key: %s", value)
```

To handle concurrent read and write operations, refer to the example in the `examples/race` directory. It demonstrates the use of goroutines to perform operations concurrently. Always use appropriate synchronization mechanisms like mutexes or channels to ensure thread safety in concurrent environments.

### Testing

To run the tests included with enchanted-sleeve, make sure you have installed Go and configured your environment. Run the following command from the root of the project directory:

```
go test ./...
```

This will execute all tests across all packages in the repository.

### Contributing

We welcome contributions to enchanted-sleeve! To contribute:

- Submit issues to report bugs or request features.
- Propose changes via pull requests (PRs), following the Pull Request process.
- Follow the code of conduct and the coding style guidelines of the project.

Always write meaningful commit messages and provide context for your changes.

### Contact

For questions and support, please open an issue in the project's GitHub repository.

### License

enchanted-sleeve is licensed under the MIT License - see the LICENSE file for details.