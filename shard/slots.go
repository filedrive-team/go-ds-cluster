package shard

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"

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
	remain      uint16
}

type Node struct {
	ID    string     `json:"id"`
	Slots SlotsRange `json:"slots"`
}

type SlotsRange struct {
	Start uint16 `json:"start"`
	End   uint16 `json:"end"`
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
	// how may slots in a range, the real number would be adjusted if have remain
	sm.rangeLen = SLOTS_NUM / sm.nodesNum
	// the leftover slots which can't cover all nodes, should allocate these leftover to nodes as distributed as possible
	sm.remain = SLOTS_NUM % sm.nodesNum
	if sm.remain > 0 {
		// decide which node should be allocated one leftover
		sm.rangeFactor = float64(nodeLen) / float64(sm.remain)
	}
	// allocate remain to slotsRange according to adjustMap
	adjustMap := make(map[uint16][]uint16)
	// slotsRange been allocated remain is lucky
	luckyMap := make(map[uint16]uint16)
	var i uint16
	for ; i < sm.remain; i++ {
		luckyMap[uint16(math.Floor((float64(i)+0.5)*sm.rangeFactor))] = i
	}

	var allocatedRemain uint16
	for i = 0; i < uint16(len(sm.nodes)); i++ {
		if _, ok := luckyMap[i]; ok {
			adjustMap[i] = []uint16{allocatedRemain, allocatedRemain + 1}
			allocatedRemain += 1
		} else {
			adjustMap[i] = []uint16{allocatedRemain, allocatedRemain}
		}
	}

	for i := range sm.slotsRange {
		adjust := adjustMap[uint16(i)]
		sm.slotsRange[i] = SlotsRange{
			Start: sm.rangeLen*uint16(i) + adjust[0],
			End:   sm.rangeLen*(uint16(i+1)) - 1 + adjust[1],
		}
		sm.nodes[i].Slots = sm.slotsRange[i]
		sm.nodeMap[i] = sm.nodes[i]
	}

	// var factorNext float64 = 0
	// for i := range sm.slotsRange {
	// 	var start, end uint16
	// 	if i == 0 {
	// 		start = 0
	// 	} else {
	// 		start = sm.slotsRange[i-1].End + 1
	// 	}

	// 	end = start + sm.rangeLen - 1
	// 	//fmt.Println(factorNext)
	// 	//fmt.Printf("l1: %f l2 %f l3: %f \n", float64(i+1), (factorNext+0.5)*sm.rangeFactor, float64(i+1)-(factorNext+0.5)*sm.rangeFactor)
	// 	if sm.remain > 0 && float64(i+1)-(factorNext+0.5)*sm.rangeFactor > 0 {
	// 		factorNext = factorNext + 1
	// 		end++
	// 	}

	// 	if i+1 == len(sm.slotsRange) {
	// 		end = SLOTS_NUM - 1
	// 	}
	// 	sm.slotsRange[i] = SlotsRange{
	// 		Start: start,
	// 		End:   end,
	// 	}
	// 	sm.nodes[i].Slots = sm.slotsRange[i]
	// 	sm.nodeMap[i] = sm.nodes[i]
	// }

	return sm
}

func RestoreSlotsManager(nds []Node) (*SlotsManager, error) {
	startNds := make([]Node, len(nds))
	copy(startNds, nds)
	sm := InitSlotManager(startNds)
	if !reflect.DeepEqual(sm.nodes, nds) {
		return nil, xerrors.Errorf("restore slots manager failed. expected %v, got %v", nds, sm.nodes)
	}
	return sm, nil
}

func (sm *SlotsManager) NodeByKey(key string) (*Node, error) {
	// figure out slot number
	slotN := CRC16Sum(key) & (SLOTS_NUM - 1)
	return sm.NodeBySlot(slotN)
}

func (sm *SlotsManager) NodeBySlot(n uint16) (*Node, error) {
	slotN := n % SLOTS_NUM
	// check if slotN out of range
	if slotN >= (sm.nodesNum * sm.rangeLen) {
		slotN = slotN - sm.remain
	}
	// figure out range number
	rn := slotN / sm.rangeLen
	if int(rn) >= len(sm.slotsRange) {
		return nil, xerrors.Errorf("unexpected slot number,n: %d, slotN: %d, rn: %d, sm.rangeLen: %d, len(sm.slotsRange):%d", n, slotN, rn, sm.rangeLen, len(sm.slotsRange))
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
	// for i, sr := range sm.slotsRange {
	// 	fmt.Printf("slot range %d start: %d, end: %d, num: %d\n", i, sr.Start, sr.End, sr.End-sr.Start+1)
	// }
	srb, _ := json.Marshal(sm.slotsRange)
	fmt.Printf("%s\n", srb)
	// for k, v := range sm.nodeMap {
	// 	fmt.Printf("node map key: %d, node id: %s \n", k, v.ID)
	// }
	nb, _ := json.Marshal(sm.nodes)
	fmt.Printf("%s\n", nb)
	// for _, n := range sm.nodes {
	// 	fmt.Printf("node id: %s\n", n.ID)
	// }

}

func (sm *SlotsManager) Nodes() []Node {
	return sm.nodes
}
