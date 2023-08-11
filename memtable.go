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
	// Implementation of the Insert method for the AVL tree
}

func (t *AVLTree) Delete(key int) {
	// Implementation of the Delete method for the AVL tree
}

func (t *AVLTree) Search(key int) string {
	// Implementation of the Search method for the AVL tree
}

func (t *AVLTree) rotateLeft(y *Node) *Node {
	// Implementation of the rotateLeft method for the AVL tree
}

func (t *AVLTree) rotateRight(y *Node) *Node {
	// Implementation of the rotateRight method for the AVL tree
}

func (t *AVLTree) getBalance(n *Node) int {
	// Implementation of the getBalance method for the AVL tree
}

func (t *AVLTree) minValueNode(n *Node) *Node {
	// Implementation of the minValueNode method for the AVL tree
}

func (t *AVLTree) maxValueNode(n *Node) *Node {
	// Implementation of the maxValueNode method for the AVL tree
}

func (m *Memtable) Insert(key int, value string) {
	// Implementation of the Insert method for the memtable
}

func (m *Memtable) Delete(key int) {
	// Implementation of the Delete method for the memtable
}

func (m *Memtable) Search(key int) string {
	// Implementation of the Search method for the memtable
}

func (m *Memtable) Flush() {
	// Implementation of the Flush method for the memtable
}