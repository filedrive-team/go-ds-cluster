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

var expectedSlotsRanges map[int][]SlotsRange = map[int][]SlotsRange{
	1: {
		{0, 16383},
	},
	2: {
		{0, 8191},
		{8192, 16383},
	},
	3: {
		{0, 5460},
		{5461, 10922},
		{10923, 16383},
	},
	5: {
		{0, 3276},
		{3277, 6553},
		{6554, 9829},
		{9830, 13106},
		{13107, 16383},
	},
	7: {
		{0, 2340},
		{2341, 4680},
		{4681, 7021},
		{7022, 9361},
		{9362, 11702},
		{11703, 14042},
		{14043, 16383},
	},
}

func isEqualRange(a, b []SlotsRange) bool {
	if len(a) != len(b) {
		return false
	}
	for i, sr := range a {
		srb := b[i]
		if sr.Start != srb.Start {
			return false
		}
		if sr.End != srb.End {
			return false
		}
	}
	return true
}

func TestInitSlotsManager(t *testing.T) {
	errmsg := "wrong range allocation"
	// test 1 node
	sm1 := InitSlotManager(nodeFactory(1))
	sm1.Check()
	if !isEqualRange(sm1.slotsRange, expectedSlotsRanges[1]) {
		t.Fatal(errmsg)
	}

	// test 2 node
	sm2 := InitSlotManager(nodeFactory(2))
	sm2.Check()
	if !isEqualRange(sm2.slotsRange, expectedSlotsRanges[2]) {
		t.Fatal(errmsg)
	}

	// test 3 node
	sm3 := InitSlotManager(nodeFactory(3))
	sm3.Check()
	if !isEqualRange(sm3.slotsRange, expectedSlotsRanges[3]) {
		t.Fatal(errmsg)
	}

	// test 5 node
	sm5 := InitSlotManager(nodeFactory(5))
	sm5.Check()
	if !isEqualRange(sm5.slotsRange, expectedSlotsRanges[5]) {
		t.Fatal(errmsg)
	}

	// test 7 node
	sm7 := InitSlotManager(nodeFactory(7))
	sm7.Check()
	if !isEqualRange(sm7.slotsRange, expectedSlotsRanges[7]) {
		t.Fatal(errmsg)
	}
}

func TestNodeByKey(t *testing.T) {
	sm7 := InitSlotManager(nodeFactory(7))
	sm7.Check()

	nd, err := sm7.NodeByKey("filedag")
	if err != nil {
		t.Fatal(err)
	}
	if nd.Slots.Start != 7022 && nd.Slots.End != 9361 {
		t.Fatal("unexpected node")
	}

	nd, err = sm7.NodeByKey("/providers/CIQAAKFLOWAUFV2ZUI7ZIPQN76UV73UUWSVSVXCB22L6CH5K3F4J7ZQ/AASAQAISECO6C4ACMIE3DVPVQCA6OXIVOYI3WE5QTJG6Z3RKDLM5C6SNLDVHY")
	if err != nil {
		t.Fatal(err)
	}
	if nd.Slots.Start != 9362 && nd.Slots.End != 11702 {
		t.Fatal("unexpected node")
	}
}

func TestNodeBySlot(t *testing.T) {
	sm7 := InitSlotManager(nodeFactory(7))
	sm7.Check()

	nd, err := sm7.NodeBySlot(0)
	if err != nil {
		t.Fatal(err)
	}
	if nd.Slots.Start != 0 && nd.Slots.End != 2340 {
		t.Fatal("unexpected node")
	}

	nd, err = sm7.NodeBySlot(2339)
	if err != nil {
		t.Fatal(err)
	}
	if nd.Slots.Start != 0 && nd.Slots.End != 2340 {
		t.Fatal("unexpected node")
	}

	nd, err = sm7.NodeBySlot(2341)
	if err != nil {
		t.Fatal(err)
	}
	if nd.Slots.Start != 2341 && nd.Slots.End != 4680 {
		t.Fatal("unexpected node")
	}

	nd, err = sm7.NodeBySlot(4681)
	if err != nil {
		t.Fatal(err)
	}
	if nd.Slots.Start != 4681 && nd.Slots.End != 7021 {
		t.Fatal("unexpected node")
	}

	nd, err = sm7.NodeBySlot(14042)
	if err != nil {
		t.Fatal(err)
	}
	if nd.Slots.Start != 11703 && nd.Slots.End != 14042 {
		t.Fatal("unexpected node")
	}

	nd, err = sm7.NodeBySlot(14043)
	if err != nil {
		t.Fatal(err)
	}
	if nd.Slots.Start != 14043 && nd.Slots.End != 16383 {
		t.Fatal("unexpected node")
	}
}
