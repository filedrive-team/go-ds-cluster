package store

import (
	"context"
	"crypto/rand"
	"fmt"
	mrand "math/rand"
	"time"

	"github.com/filedrive-team/go-ds-cluster/config"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	transport "github.com/libp2p/go-libp2p-core/transport"
	libp2pquic "github.com/libp2p/go-libp2p-quic-transport"
	swarm "github.com/libp2p/go-libp2p-swarm"
)

func init() {
	swarm.DialTimeoutLocal = transport.DialTimeout
}

func makeBasicHost(listenPort string) (host.Host, error) {
	priv, _, err := crypto.GenerateECDSAKeyPair(rand.Reader)
	if err != nil {
		return nil, err
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", listenPort), fmt.Sprintf("/ip4/0.0.0.0/udp/%s/quic", listenPort)),
		libp2p.Identity(priv),
		libp2p.DisableRelay(),
		libp2p.DefaultTransports,
		libp2p.Transport(libp2pquic.NewTransport),
	}

	return libp2p.New(context.Background(), opts...)
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
		libp2p.DefaultTransports,
		libp2p.Transport(libp2pquic.NewTransport),
	}
	h, err := libp2p.New(context.Background(), opts...)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func randPortNumber() string {
	mrand.Seed(time.Now().Unix() * int64(mrand.Intn(9999)))
	r := mrand.Float64()
	m := 4000 + 6000*r
	return fmt.Sprintf("%.0f", m)
}
