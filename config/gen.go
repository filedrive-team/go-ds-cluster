package config

import (
	"crypto/rand"
	"fmt"

	"github.com/filedrive-team/go-ds-cluster/shard"
	"github.com/filedrive-team/go-ds-cluster/utils"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
)

// GenClusterConf
// Generate config json files for server nodes in cluster
// num - how many servers in the cluster
//  - will generate [num] json files
//  - each config has different Identity and Address but has same Nodes info
//  - Nodes hold all info about server nodes in cluster
func GenClusterConf(num int) ([]*Config, error) {
	nodeIdentities := make([]Identity, num)
	for i := range nodeIdentities {
		priv, _, err := crypto.GenerateECDSAKeyPair(rand.Reader)
		if err != nil {
			return nil, err
		}
		sk, err := priv.Bytes()
		if err != nil {
			return nil, err
		}

		pid, err := peer.IDFromPrivateKey(priv)
		if err != nil {
			return nil, err
		}
		nodeIdentities[i] = Identity{
			PeerID: pid.Pretty(),
			SK:     sk,
		}
	}
	shardStartNodes := make([]shard.Node, num)
	for i := range shardStartNodes {
		shardStartNodes[i] = shard.Node{
			ID: nodeIdentities[i].PeerID,
		}
	}
	shardStartNodes = shard.InitSlotManager(shardStartNodes).Nodes()

	cfgNodes := make([]Node, num)
	for i := range cfgNodes {
		rport := utils.RandPort()
		cfgNodes[i] = Node{
			Node: shardStartNodes[i],
			Swarm: []string{
				fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", rport),
				fmt.Sprintf("/ip4/0.0.0.0/udp/%s/quic", rport),
			},
		}
	}
	res := make([]*Config, num)
	for i := range nodeIdentities {
		res[i] = &Config{
			Addresses: Addresses{
				Swarm: cfgNodes[i].Swarm,
			},
			Identity: nodeIdentities[i],
			Nodes:    cfgNodes,
		}
	}
	return res, nil
}

// GenClientConf
// Generate config json for cluster client node
// 	- key pair
//  - libp2p listen address
func GenClientConf() (*Config, error) {
	priv, _, err := crypto.GenerateECDSAKeyPair(rand.Reader)
	if err != nil {
		return nil, err
	}
	sk, err := priv.Bytes()
	if err != nil {
		return nil, err
	}

	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return nil, err
	}

	rport := utils.RandPort()
	return &Config{
		Addresses: Addresses{
			Swarm: []string{
				fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", rport),
				fmt.Sprintf("/ip4/0.0.0.0/udp/%s/quic", rport),
			},
		},
		Identity: Identity{
			PeerID: pid.Pretty(),
			SK:     sk,
		},
		Nodes: make([]Node, 0),
	}, nil
}
