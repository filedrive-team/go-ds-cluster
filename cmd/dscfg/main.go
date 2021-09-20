package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"path"
	"time"

	"github.com/filedrive-team/go-ds-cluster/config"
	"github.com/filedrive-team/go-ds-cluster/shard"
	log "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var logging = log.Logger("dscfg")

func init() {
	log.SetLogLevel("*", "INFO")
}

func main() {
	local := []*cli.Command{
		clientCmd,
		clusterCmd,
	}

	app := &cli.App{
		Name:     "dscfg",
		Commands: local,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
}

var clusterCmd = &cli.Command{
	Name:  "cluster",
	Usage: "generate cluster config files",
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:  "cluster-node-number",
			Value: 3,
			Usage: "specify the cluster node number",
		},
	},
	Action: func(c *cli.Context) error {
		outdir := c.Args().First()
		if outdir == "" {
			return xerrors.Errorf("usage: dscfg client [output-dir]")
		}
		finfo, err := os.Stat(outdir)
		if err != nil {
			return err
		}
		if !finfo.IsDir() {
			return xerrors.Errorf("usage: dscfg client [output-dir]")
		}
		nodeNum := c.Int("cluster-node-number")
		nodeIdentities := make([]config.Identity, nodeNum)
		for i := range nodeIdentities {
			priv, _, err := crypto.GenerateECDSAKeyPair(rand.Reader)
			if err != nil {
				return err
			}
			sk, err := priv.Bytes()
			if err != nil {
				return err
			}

			pid, err := peer.IDFromPrivateKey(priv)
			if err != nil {
				return err
			}
			nodeIdentities[i] = config.Identity{
				PeerID: pid.Pretty(),
				SK:     sk,
			}
		}
		shardStartNodes := make([]shard.Node, nodeNum)
		for i := range shardStartNodes {
			shardStartNodes[i] = shard.Node{
				ID: nodeIdentities[i].PeerID,
			}
		}
		shardStartNodes = shard.InitSlotManager(shardStartNodes).Nodes()

		cfgNodes := make([]config.Node, nodeNum)
		for i := range cfgNodes {
			rport := randPortNumber()
			cfgNodes[i] = config.Node{
				Node: shardStartNodes[i],
				Swarm: []string{
					fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", rport),
					fmt.Sprintf("/ip4/0.0.0.0/udp/%s/quic", rport),
				},
			}
		}

		for i := range nodeIdentities {
			clientCfg := &config.Config{
				Addresses: config.Addresses{
					Swarm: cfgNodes[i].Swarm,
				},
				Identity: nodeIdentities[i],
				Nodes:    cfgNodes,
			}
			cfgbytes, err := json.MarshalIndent(&clientCfg, "", "\t")
			if err != nil {
				return err
			}
			err = ioutil.WriteFile(path.Join(outdir, fmt.Sprintf("cluster_%02d.json", i)), cfgbytes, 0644)
			if err != nil {
				return err
			}
		}
		return nil
	},
}

var clientCmd = &cli.Command{
	Name:  "client",
	Usage: "generate a client config file",
	Flags: []cli.Flag{},
	Action: func(c *cli.Context) error {
		outpath := c.Args().First()
		if outpath == "" {
			return xerrors.Errorf("usage: dscfg client [output]")
		}
		priv, _, err := crypto.GenerateECDSAKeyPair(rand.Reader)
		if err != nil {
			return err
		}
		sk, err := priv.Bytes()
		if err != nil {
			return err
		}

		pid, err := peer.IDFromPrivateKey(priv)
		if err != nil {
			return err
		}

		rport := randPortNumber()
		clientCfg := &config.Config{
			Addresses: config.Addresses{
				Swarm: []string{
					fmt.Sprintf("/ip4/0.0.0.0/tcp/%s", rport),
					fmt.Sprintf("/ip4/0.0.0.0/udp/%s/quic", rport),
				},
			},
			Identity: config.Identity{
				PeerID: pid.Pretty(),
				SK:     sk,
			},
			Nodes: make([]config.Node, 0),
		}
		cfgbytes, err := json.MarshalIndent(&clientCfg, "", "\t")
		if err != nil {
			return err
		}
		return ioutil.WriteFile(outpath, cfgbytes, 0644)
	},
}

func randPortNumber() string {
	mrand.Seed(time.Now().Unix() * int64(mrand.Intn(9999)))
	r := mrand.Float64()
	m := 1000 + 9000*r
	return fmt.Sprintf("%.0f", m)
}
