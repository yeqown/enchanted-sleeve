package main

import (
	"fmt"
	"os"
)

type Node struct {
	key   int
	value string
	left  *Node
	right *Node
	height int
}

type AVLTree struct {
	root *Node
}

type Memtable struct {
	tree *AVLTree
	sizeLimit int
}

func (t *AVLTree) Insert(key int, value string) {
	// TODO: Implement the Insert method for the AVL tree
}

func (t *AVLTree) Delete(key int) {
	// TODO: Implement the Delete method for the AVL tree
}

func (t *AVLTree) Search(key int) string {
	// TODO: Implement the Search method for the AVL tree
}

func (t *AVLTree) rotateLeft(y *Node) *Node {
	// TODO: Implement the rotateLeft method for the AVL tree
}

func (t *AVLTree) rotateRight(y *Node) *Node {
	// TODO: Implement the rotateRight method for the AVL tree
}

func (t *AVLTree) getBalance(n *Node) int {
	// TODO: Implement the getBalance method for the AVL tree
}

func (t *AVLTree) minValueNode(n *Node) *Node {
	// TODO: Implement the minValueNode method for the AVL tree
}

func (t *AVLTree) maxValueNode(n *Node) *Node {
	// TODO: Implement the maxValueNode method for the AVL tree
}

func (m *Memtable) Insert(key int, value string) {
	// TODO: Implement the Insert method for the memtable
}

func (m *Memtable) Delete(key int) {
	// TODO: Implement the Delete method for the memtable
}

func (m *Memtable) Search(key int) string {
	// TODO: Implement the Search method for the memtable
}

func (m *Memtable) Flush() {
	// TODO: Implement the Flush method for the memtable
}
