package main

import (
	"os"
	"fmt"
)

type Memtable struct {
	data map[string]string
	size int
}

func (m *Memtable) Insert(key, value string) {
	m.data[key] = value
	m.size += len(key) + len(value)
	if m.size > 1024 { // Assuming the size limit is 1024
		m.Flush()
	}
}

func (m *Memtable) Delete(key string) {
	delete(m.data, key)
}

func (m *Memtable) Search(key string) (string, bool) {
	value, exists := m.data[key]
	return value, exists
}

func (m *Memtable) Flush() {
	file, _ := os.Create("memtable.txt")
	defer file.Close()

	for key, value := range m.data {
		fmt.Fprintln(file, key, value)
	}

	m.data = make(map[string]string)
	m.size = 0
}

type AVLNode struct {
	key   string
	value string
	left  *AVLNode
	right *AVLNode
	height int
}

type AVLTree struct {
	root *AVLNode
}

// Implement AVL tree methods here
func (t *AVLTree) Insert(key, value string) {
	// Insert method implementation
}

func (t *AVLTree) Delete(key string) {
	// Delete method implementation
}

func (t *AVLTree) Search(key string) (*AVLNode, bool) {
	// Search method implementation
}

func (t *AVLTree) balance(node *AVLNode) *AVLNode {
	// Balance method implementation
}

func main() {
	m := &Memtable{
		data: make(map[string]string),
		size: 0,
	}

	// Use the memtable here
	// Memtable usage implementation
}