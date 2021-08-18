package store

import (
	"context"
	"crypto/rand"
	"fmt"
	"testing"

	cborutil "github.com/filecoin-project/go-cbor-util"
	ds "github.com/ipfs/go-datastore"
	log "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
)

func TestP2P(t *testing.T) {
	log.SetLogLevel("*", "info")
	h1, err := makeBasicHost(3220)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := makeBasicHost(3330)
	if err != nil {
		t.Fatal(err)
	}
	h2Info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h2.SetStreamHandler(PROTOCOL_REQUEST_V1, func(s network.Stream) {
		defer s.Close()
		logging.Info("incoming stream")
		var hmsg RequestMessage
		if err := cborutil.ReadCborRPC(s, &hmsg); err != nil {
			t.Error(err)
			_ = s.Conn().Close()
			return
		}
		logging.Infof("%s", hmsg)

		reply := &ReplyMessage{
			Msg: "ok",
		}
		if err := cborutil.WriteCborRPC(s, reply); err != nil {
			t.Error(err)
		}
	})

	h1.Peerstore().AddAddrs(h2Info.ID, h2Info.Addrs, peerstore.PermanentAddrTTL)

	// err = h1.Connect(ctx, h2Info)
	// if err != nil {
	// 	t.Error(err)
	// }

	s, err := h1.NewStream(ctx, h2Info.ID, PROTOCOL_REQUEST_V1)
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
		t.Error(err)
	}

	reply := new(ReplyMessage)
	if err := cborutil.ReadCborRPC(s, reply); err != nil {
		t.Error(err)
		_ = s.Conn().Close()
		return
	}
	logging.Infof("%v", *reply)
}

func TestDataNode(t *testing.T) {
	log.SetLogLevel("*", "info")
	h1, err := makeBasicHost(3220)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := makeBasicHost(3330)
	if err != nil {
		t.Fatal(err)
	}
	h2Info := peer.AddrInfo{
		ID:    h2.ID(),
		Addrs: h2.Addrs(),
	}

	ctx := context.Background()
	memStore := ds.NewMapDatastore()
	defer memStore.Close()

	server := NewStoreServer(ctx, h2, PROTOCOL_REQUEST_V1, PROTOCOL_REPLY_V1, memStore)
	defer server.Close()
	go server.Serve()

	client := NewStoreClient(ctx, h1, h2Info, PROTOCOL_REQUEST_V1, PROTOCOL_REPLY_V1)
	defer client.Close()
	go client.SetHandle()

	keep := make(chan struct{})
	go func() {
		defer func() {
			keep <- struct{}{}
		}()
		//err = client.Put("winner", []byte("Leo is a good boy!"))
		b, err := client.Get("winner")
		if err != nil {
			t.Error(err)
		}
		logging.Info(b)

	}()
	<-keep
	t.Fail()
	// size, err := client.GetSize("winner")
	// if err != nil {
	// 	t.Fatal(err)
	// }
	// if size != 3 {
	// 	t.Fatal(xerrors.New("size not match"))
	// }
}

func makeBasicHost(listenPort int) (host.Host, error) {
	priv, _, err := crypto.GenerateECDSAKeyPair(rand.Reader)
	if err != nil {
		return nil, err
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%d", listenPort)),
		libp2p.Identity(priv),
		libp2p.DisableRelay(),
	}

	return libp2p.New(context.Background(), opts...)
}
