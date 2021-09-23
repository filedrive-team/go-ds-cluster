package config

import (
	"testing"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
)

func TestGenClientConf(t *testing.T) {
	cfg, err := GenClientConf()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Identity.PeerID == "" {
		t.Fatal("empty peer id")
	}
	if len(cfg.Identity.SK) == 0 {
		t.Fatal("empty secret key")
	}
	if len(cfg.Addresses.Swarm) == 0 {
		t.Fatal("empty swarm address")
	}
	pid, err := peer.Decode(cfg.Identity.PeerID)
	if err != nil {
		t.Fatal(err)
	}
	priv, err := crypto.UnmarshalPrivateKey(cfg.Identity.SK)
	if err != nil {
		t.Fatal(err)
	}
	ppid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		t.Fatal(err)
	}
	if pid.String() != ppid.String() {
		t.Fatal("pid should be the same")
	}
}
