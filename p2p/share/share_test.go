package share

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/filedrive-team/go-ds-cluster/config"
	"github.com/filedrive-team/go-ds-cluster/p2p"
	"github.com/filedrive-team/go-ds-cluster/utils"
	"github.com/libp2p/go-libp2p-core/peer"
)

var testNodesInfo = `[{"id":"12D3KooWM1dWYafTFGJc6Kq5XYX6RRbQTCbZ58kXFWsjdHREJtCB","slots":{"start":0,"end":5460},"swarm":["/ip4/0.0.0.0/tcp/9690"]},{"id":"12D3KooWQHrRAyak1wYc9u27Tu9HnAqkafdWhZkaohSP834igaiZ","slots":{"start":5461,"end":10922},"swarm":["/ip4/0.0.0.0/tcp/9691"]},{"id":"12D3KooWQWLzFTEE9XD2oZph4UifRkE4BWiapsqHjWMnB1R5WRtS","slots":{"start":10923,"end":16383},"swarm":["/ip4/0.0.0.0/tcp/9692"]}]`

var identitystr = `{"peer_id":"QmTtSXcwN9BiyWJcLrcdRRKt13tNQk8DvTNKZZC6saMaHV","sk":"CAMSeTB3AgEBBCBsED0AQeHlxwEzU1xq0W79ijOwDvCMIYioAX9XhVfkHaAKBggqhkjOPQMBB6FEA0IABIbPiejDx3vct0c+eN1TIqMXJyP5dvzKmZAWf0MXyqBJAGTRKRTvFCY4Es4E9MCWzfDjI2fG1KluAg8iiNTH0fY="}`

func TestShareNode(t *testing.T) {
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

	var cfgNodes = make([]config.Node, 0)
	err = json.Unmarshal([]byte(testNodesInfo), &cfgNodes)
	if err != nil {
		t.Fatal(err)
	}
	var identity config.Identity
	err = json.Unmarshal([]byte(identitystr), &identity)
	if err != nil {
		t.Fatal(err)
	}

	server := NewShareServer(ctx, h2, &config.Config{
		Nodes:        cfgNodes,
		IdentityList: []config.Identity{identity},
	})
	defer server.Close()
	server.Serve()

	client := NewShareClient(ctx, h1, h2Info)
	defer client.Close()

	// test get cluster info
	res, err := client.GetClusterInfo()
	if err != nil {
		t.Fatal(err)
	}

	if string(res) != testNodesInfo {
		t.Fatal("cluster info not match")
	}

	// test get identity
	bs, err := client.GetIdentity(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(bs) != identitystr {
		t.Fatal("identity not match")
	}
}
