package shard

import (
	"fmt"

	"golang.org/x/xerrors"
)

const (
	SLOTS_NUM = 1 << 14
)

type SlotsManager struct {
	nodesNum    uint16
	rangeLen    uint16
	rangeFactor float64
	nodes       []Node
	slotsRange  []SlotsRange
	nodeMap     map[int]Node
}

type Node struct {
	ID    string
	Slots SlotsRange
}

type SlotsRange struct {
	Start uint16
	End   uint16
}

func InitSlotManager(startNodes []Node) *SlotsManager {
	nodeLen := len(startNodes)
	sm := &SlotsManager{
		nodes:      startNodes,
		nodesNum:   uint16(nodeLen),
		slotsRange: make([]SlotsRange, len(startNodes)),
		nodeMap:    make(map[int]Node),
	}
	// generate slots range
	sm.rangeLen = SLOTS_NUM / sm.nodesNum
	remine := SLOTS_NUM % sm.nodesNum
	if remine > 0 {
		sm.rangeFactor = float64(nodeLen) / float64(remine)
	}
	fmt.Printf("remine %d, rf: %f\n", remine, sm.rangeFactor)

	var factorNext float64 = 0
	for i := range sm.slotsRange {
		var start, end uint16
		if i == 0 {
			start = 0
		} else {
			start = sm.slotsRange[i-1].End + 1
		}

		end = start + sm.rangeLen - 1
		fmt.Println(factorNext)
		fmt.Printf("l1: %f l2 %f l3: %f \n", float64(i+1), (factorNext+0.5)*sm.rangeFactor, float64(i+1)-(factorNext+0.5)*sm.rangeFactor)
		if remine > 0 && float64(i+1)-(factorNext+0.5)*sm.rangeFactor > 0 {
			factorNext = factorNext + 1
			end++
		}

		if i+1 == len(sm.slotsRange) {
			end = SLOTS_NUM - 1
		}
		sm.slotsRange[i] = SlotsRange{
			Start: start,
			End:   end,
		}
		sm.nodes[i].Slots = sm.slotsRange[i]
		sm.nodeMap[i] = sm.nodes[i]
	}

	return sm
}

func (sm *SlotsManager) NodeByKey(key string) (*Node, error) {
	// figure out slot number
	slotN := CRC16Sum(key) & (SLOTS_NUM - 1)
	return sm.NodeBySlot(slotN)
}

func (sm *SlotsManager) NodeBySlot(n uint16) (*Node, error) {
	slotN := n
	// figure out range number
	rn := slotN / sm.rangeLen
	if int(rn) >= len(sm.slotsRange) {
		return nil, xerrors.Errorf("unexpected slot number")
	}
	maybeRange := sm.slotsRange[rn]
	// check if really in the range
	if slotN < maybeRange.Start {
		rn--
	} else if slotN > maybeRange.End {
		rn++
	}
	if node, ok := sm.nodeMap[int(rn)]; ok {
		return &node, nil
	}
	return nil, xerrors.Errorf("failed to find node by slot: %d; rangeN: %d", slotN, rn)
}

func (sm *SlotsManager) Check() {
	fmt.Printf("nodes: %d\n", sm.nodesNum)
	fmt.Printf("range len: %d\n", sm.rangeLen)
	for i, sr := range sm.slotsRange {
		fmt.Printf("slot range %d start: %d, end: %d, num: %d\n", i, sr.Start, sr.End, sr.End-sr.Start+1)
	}
	for k, v := range sm.nodeMap {
		fmt.Printf("node map key: %d, node id: %s \n", k, v.ID)
	}
	for _, n := range sm.nodes {
		fmt.Printf("node id: %s\n", n.ID)
	}
}
