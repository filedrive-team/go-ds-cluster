package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"

	"github.com/filedrive-team/go-ds-cluster/config"
	log "github.com/ipfs/go-log/v2"
	"github.com/mitchellh/go-homedir"
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
			Name:     "cluster-node-number",
			Aliases:  []string{"nn"},
			Required: true,
			Usage:    "specify the cluster node number",
		},
	},
	Action: func(c *cli.Context) error {
		outdir := c.Args().First()
		if outdir == "" {
			return xerrors.Errorf("usage: dscfg client [output-dir]")
		}
		outdir, err := homedir.Expand(outdir)
		if err != nil {
			return err
		}
		err = os.MkdirAll(outdir, 0755)
		if err != nil {
			return err
		}

		nodeNum := c.Int("cluster-node-number")
		clustercfg, err := config.GenClusterConf(nodeNum)
		if err != nil {
			return err
		}

		cfgbytes, err := json.MarshalIndent(&clustercfg, "", "\t")
		if err != nil {
			return err
		}
		err = ioutil.WriteFile(path.Join(outdir, config.DefaultConfigJson), cfgbytes, 0644)
		if err != nil {
			return err
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
		clientCfg, err := config.GenClientConf()
		if err != nil {
			return err
		}
		cfgbytes, err := json.MarshalIndent(clientCfg, "", "\t")
		if err != nil {
			return err
		}
		return ioutil.WriteFile(outpath, cfgbytes, 0644)
	},
}
