package store

import (
	"context"
	"crypto/rand"
	"fmt"

	"github.com/filedrive-team/go-ds-cluster/config"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
)

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

func BasicHost(listenPort int) (host.Host, error) {
	return makeBasicHost(listenPort)
}

func HostFromConf(cfg *config.Config) (host.Host, error) {
	priv, err := crypto.UnmarshalPrivateKey(cfg.Identity.SK)
	if err != nil {
		return nil, err
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(cfg.Addresses.Swarm...),
		libp2p.Identity(priv),
		libp2p.DisableRelay(),
	}
	h, err := libp2p.New(context.Background(), opts...)
	if err != nil {
		return nil, err
	}

	return h, nil
}
