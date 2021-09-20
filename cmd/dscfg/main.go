package main

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	mrand "math/rand"
	"os"
	"time"

	"github.com/filedrive-team/go-ds-cluster/config"
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
	mrand.Seed(time.Now().Unix())
	r := mrand.Float64()
	m := 1000 + 9000*r
	return fmt.Sprintf("%.0f", m)
}
