package store

import (
	"bytes"
	"context"
	"testing"

	"github.com/filedrive-team/go-ds-cluster/utils"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-core/peer"
)

type Pair struct {
	K string
	V []byte
}

var tdata = []Pair{
	{"Filedrive", []byte("Platform for better use of datasets on web3")},
	{"FileDAG", []byte("Destributed storage provider")},
	{"afsis", []byte("Africa Soil Information Service (AfSIS) Soil Chemistry")},
}

/*
func TestP2P(t *testing.T) {
	log.SetLogLevel("*", "info")
	h1, err := makeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2, err := makeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2Info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h2.SetStreamHandler(PROTOCOL_V1, func(s network.Stream) {
		defer s.Close()
		var hmsg RequestMessage
		if err := cborutil.ReadCborRPC(s, &hmsg); err != nil {
			t.Fatal(err)
		}
		logging.Infof("%s", hmsg)

		reply := &ReplyMessage{
			Msg: "ok",
		}
		if err := cborutil.WriteCborRPC(s, reply); err != nil {
			t.Fatal(err)
		}
	})

	h1.Peerstore().AddAddrs(h2Info.ID, h2Info.Addrs, peerstore.PermanentAddrTTL)

	s, err := h1.NewStream(ctx, h2Info.ID, PROTOCOL_V1)
	if err != nil {
		t.Error(err)
	}
	defer s.Close()

	req := &RequestMessage{
		Key:    "winner",
		Value:  []byte("leo"),
		Action: ActPut,
	}
	if err := cborutil.WriteCborRPC(s, req); err != nil {
		t.Fatal(err)
	}

	reply := new(ReplyMessage)
	if err := cborutil.ReadCborRPC(s, reply); err != nil {
		t.Fatal(err)
	}
}
*/

func TestDisableDelete(t *testing.T) {
	h1, err := makeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2, err := makeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2Info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	ctx := context.Background()
	memStore := ds.NewMapDatastore()

	server := NewStoreServer(ctx, h2, PROTOCOL_V1, memStore, true)
	defer server.Close()
	server.Serve()

	client := NewStoreClient(ctx, h1, h2Info, PROTOCOL_V1)
	defer client.Close()

	for _, d := range tdata {
		err = client.Put(d.K, d.V)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, d := range tdata {
		err := client.Delete(d.K)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range tdata {
		has, err := client.Has(d.K)
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatalf("should have data %s", d.K)
		}
	}

}

func TestDataNode(t *testing.T) {
	h1, err := makeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2, err := makeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2Info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	ctx := context.Background()
	memStore := ds.NewMapDatastore()

	server := NewStoreServer(ctx, h2, PROTOCOL_V1, memStore, false)
	defer server.Close()
	server.Serve()

	client := NewStoreClient(ctx, h1, h2Info, PROTOCOL_V1)
	defer client.Close()

	for _, d := range tdata {
		err = client.Put(d.K, d.V)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, d := range tdata {
		has, err := client.Has(d.K)
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatalf("should have data %s", d.K)
		}
	}

	for _, d := range tdata {
		size, err := client.GetSize(d.K)
		if err != nil {
			t.Fatal(err)
		}
		if size != len(d.V) {
			t.Fatalf("%s size not match", d.K)
		}
	}

	for _, d := range tdata {
		v, err := client.Get(d.K)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v, d.V) {
			t.Fatalf("%s value not match", d.K)
		}
	}

	for _, d := range tdata {
		err := client.Delete(d.K)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range tdata {
		has, err := client.Has(d.K)
		if err != nil {
			t.Fatal(err)
		}
		if has {
			t.Fatalf("should not have data %s", d.K)
		}
	}

}

func TestDataNodeQuery(t *testing.T) {
	h1, err := makeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2, err := makeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2Info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	ctx := context.Background()
	memStore := ds.NewMapDatastore()

	server := NewStoreServer(ctx, h2, PROTOCOL_V1, memStore, false)
	defer server.Close()
	server.Serve()

	client := NewStoreClient(ctx, h1, h2Info, PROTOCOL_V1)
	defer client.Close()

	for _, d := range tdata {
		err = client.Put(d.K, d.V)
		if err != nil {
			t.Fatal(err)
		}
	}

	results, err := client.Query(dsq.Query{})
	if err != nil {
		t.Fatal(err)
	}
	ents, err := results.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != len(tdata) {
		t.Fatalf("count of data not match, expected count: %d, got: %d", len(tdata), len(ents))
	}
	for _, d := range tdata {
		ent, ok := findEntry(ds.NewKey(d.K), ents)
		if !ok {
			t.Fatal("should found data from query result")
		}
		if !bytes.Equal(ent.Value, d.V) {
			t.Fatalf("unexpected value, expected: %s, got: %s", d.V, ent.Value)
		}
	}
}

func findEntry(k ds.Key, ents []dsq.Entry) (dsq.Entry, bool) {
	for _, ent := range ents {
		if ent.Key == k.String() {
			return ent, true
		}
	}
	return dsq.Entry{}, false
}
