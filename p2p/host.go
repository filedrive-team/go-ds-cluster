package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"io/ioutil"

	"github.com/filedrive-team/go-ds-cluster/config"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/pnet"
)

func MakeBasicHost(listenPort string) (host.Host, error) {
	priv, _, err := crypto.GenerateECDSAKeyPair(rand.Reader)
	if err != nil {
		return nil, err
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%s", listenPort)),
		libp2p.Identity(priv),
		libp2p.DisableRelay(),
		libp2p.DefaultTransports,
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
	}
	h, err := libp2p.New(context.Background(), opts...)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func MakeHost(listenPort string, keypath string) (host.Host, error) {
	priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}

	opts := []libp2p.Option{
		libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/127.0.0.1/tcp/%s", listenPort)),
		libp2p.Identity(priv),
		libp2p.DisableRelay(),
		libp2p.DefaultTransports,
	}
	if keypath != "" {
		kb, err := ioutil.ReadFile(keypath)
		if err != nil {
			return nil, err
		}
		psk, err := pnet.DecodeV1PSK(bytes.NewReader(kb))
		if err != nil {
			return nil, fmt.Errorf("failed to configure private network: %s", err)
		}
		opts = append(opts, libp2p.PrivateNetwork(psk))
	}

	return libp2p.New(context.Background(), opts...)
}
