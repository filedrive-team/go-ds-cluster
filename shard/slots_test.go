package shard

import (
	"fmt"
	"testing"
)

func nodeFactory(num int) []Node {
	r := make([]Node, num)
	for i := range r {
		r[i] = Node{
			ID: fmt.Sprintf("node-%03d", i),
		}
	}
	return r
}
func TestInitSlotsManager(t *testing.T) {
	// test 1 node
	sm1 := InitSlotManager(nodeFactory(1))
	sm1.Check()

	// test 2 node
	sm2 := InitSlotManager(nodeFactory(2))
	sm2.Check()

	// test 3 node
	sm3 := InitSlotManager(nodeFactory(3))
	sm3.Check()

	// test 5 node
	sm5 := InitSlotManager(nodeFactory(5))
	sm5.Check()

	// test 7 node
	sm7 := InitSlotManager(nodeFactory(7))
	sm7.Check()

	// test 10 node
	sm10 := InitSlotManager(nodeFactory(10))
	sm10.Check()

	// test 50 node
	sm50 := InitSlotManager(nodeFactory(50))
	sm50.Check()

	t.Fail()
}
