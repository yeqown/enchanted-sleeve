# enchanted-sleeve

enchanted-sleeve is a KV store that uses a file as a backend. It is a simple
key-value store that supports basic operations like `get`, `put`, `delete`, and
`list`.

> Enchanted sleeves is from the chinese myth item which called "蟒袖" that can 
> store things in it and what the most important is that it store things 
> more than its size, even a mountain.

### Getting started

```go
db := esl.New("path/to/file", nil)
err := db.Put([]byte("key"), []byte("value"))
_assert(err == nil)

value, err := db.Get([]byte("key"))
_assert(err == nil)
_assert(string(value) == "value")

db.Close()
```