package remoteds

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/filedrive-team/go-ds-cluster/p2p"
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

func userPrefix(req *Request, reply *ReplyMessage) bool {
	var prefix string
	switch req.Action {
	case ActTouchFile, ActFileInfo, ActDeleteFile, ActListFiles:
		prefix = filepath.Join("/", PREFIX, "ccc")
		req.InnerFileKey = filepath.Join(prefix, req.Key)
		req.InnerFilePrefix = prefix
	}
	return false
}

func TestDataNode(t *testing.T) {
	h1, err := p2p.MakeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2, err := p2p.MakeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2Info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	ctx := context.Background()
	memStore := ds.NewMapDatastore()

	server := NewStoreServer(ctx, h2, PROTOCOL_V1, memStore, memStore, false, 180, userPrefix)
	defer server.Close()
	server.Serve()

	client := NewStoreClient(ctx, h1, h2Info, PROTOCOL_V1, 180, "")
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

	for _, d := range tdata {
		err = client.TouchFile(d.K, d.V)
		if err != nil {
			t.Fatal(err)
		}
	}

	for _, d := range tdata {
		v, err := client.FileInfo(d.K)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v, d.V) {
			t.Fatalf("%s value not match", d.K)
		}
	}

	for _, d := range tdata {
		err := client.DeleteFile(d.K)
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, d := range tdata {
		_, err := client.FileInfo(d.K)
		if err != ds.ErrNotFound {
			t.Fatal(err)
		}
	}

}

func TestDataNodeQuery(t *testing.T) {
	h1, err := p2p.MakeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2, err := p2p.MakeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2Info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	ctx := context.Background()
	memStore := ds.NewMapDatastore()

	server := NewStoreServer(ctx, h2, PROTOCOL_V1, memStore, memStore, false, 2, userPrefix)
	defer server.Close()
	server.Serve()

	client := NewStoreClient(ctx, h1, h2Info, PROTOCOL_V1, 2, "")
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
	t.Log(ents)
	for _, d := range tdata {
		ent, ok := findEntry(ds.NewKey(d.K), ents)
		if !ok {
			t.Log(d)
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

func TestListFiles(t *testing.T) {
	h1, err := p2p.MakeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2, err := p2p.MakeBasicHost(utils.RandPort())
	if err != nil {
		t.Fatal(err)
	}
	h2Info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	ctx := context.Background()
	memStore := ds.NewMapDatastore()

	server := NewStoreServer(ctx, h2, PROTOCOL_V1, memStore, memStore, false, 2, userPrefix)
	defer server.Close()
	server.Serve()

	client := NewStoreClient(ctx, h1, h2Info, PROTOCOL_V1, 2, "")
	defer client.Close()

	for _, d := range tdata {
		err = client.TouchFile(d.K, d.V)
		if err != nil {
			t.Fatal(err)
		}
	}

	results, err := client.ListFiles("")
	if err != nil {
		t.Fatal(err)
	}

	for d := range results {
		ent, ok := findData(d.Key, tdata)
		if !ok {
			t.Log(d)
			t.Fatal("should found data from query result")
		}
		if !bytes.Equal(ent.V, d.Value) {
			t.Fatalf("unexpected value, expected: %s, got: %s", d.Value, ent.V)
		}
	}
}

func findData(k string, ents []Pair) (Pair, bool) {
	for _, ent := range ents {
		if ds.NewKey(ent.K).String() == k {
			return ent, true
		}
	}
	return Pair{}, false
}
