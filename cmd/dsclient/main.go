package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/filedrive-team/filehelper"
	"github.com/filedrive-team/filehelper/dataset"
	"github.com/filedrive-team/go-ds-cluster/clusterclient"
	"github.com/filedrive-team/go-ds-cluster/config"
	"github.com/filedrive-team/go-ds-cluster/p2p"
	"github.com/filedrive-team/go-ds-cluster/p2p/share"
	"github.com/filedrive-team/go-ds-cluster/utils"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	dsmount "github.com/ipfs/go-datastore/mount"
	bstore "github.com/ipfs/go-ipfs-blockstore"
	dshelp "github.com/ipfs/go-ipfs-ds-help"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	log "github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-merkledag"
	ufsio "github.com/ipfs/go-unixfs/io"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/mitchellh/go-homedir"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

var logging = log.Logger("dsclient")

func init() {
	log.SetLogLevel("*", "INFO")
}

func main() {
	local := []*cli.Command{
		addCmd,
		importDatasetCmd,
		statCmd,
		getCmd,
		initCmd,
		hashslotCmd,
		boundCmd,
	}

	app := &cli.App{
		Name: "dsclient",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "conf",
				Usage: "specify the dscluster config path",
				Value: config.DefaultConfigPath,
			},
		},
		Commands: local,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Println("Error: ", err)
		os.Exit(1)
	}
}

var importDatasetCmd = &cli.Command{
	Name:  "import-dataset",
	Usage: "import files from the specified dataset",
	Flags: []cli.Flag{
		// &cli.StringFlag{
		// 	Name:     "dscluster",
		// 	Required: true,
		// 	Usage:    "specify the dscluster config path",
		// },
		&cli.IntFlag{
			Name:  "retry",
			Value: 5,
			Usage: "retry write file to datastore",
		},
		&cli.IntFlag{
			Name:  "retry-wait",
			Value: 1,
			Usage: "sleep time before a retry",
		},
		&cli.IntFlag{
			Name:  "parallel",
			Value: 6,
			Usage: "specify batch job number",
		},
		&cli.IntFlag{
			Name:    "batch-read-num",
			Aliases: []string{"br"},
			Value:   32,
			Usage:   "specify batch read num",
		},
	},
	Action: func(c *cli.Context) (err error) {
		ctx := context.Background()
		confPath := c.String("conf")
		confPath, err = homedir.Expand(confPath)
		if err != nil {
			return err
		}
		cfg, err := config.ReadConfig(path.Join(confPath, config.DefaultConfigJson))
		if err != nil {
			return err
		}
		//dsclusterCfg := c.String("dscluster")
		parallel := c.Int("parallel")
		batchReadNum := c.Int("batch-read-num")

		targetPath := c.Args().First()
		targetPath, err = homedir.Expand(targetPath)
		if err != nil {
			return err
		}
		if !filehelper.ExistDir(targetPath) {
			return xerrors.New("Unexpected! The path to dataset does not exist")
		}
		var ds ds.Datastore

		// cfg, err := config.ReadConfig(dsclusterCfg)
		// if err != nil {
		// 	return err
		// }
		ds, err = clusterclient.NewClusterClient(context.Background(), cfg)
		if err != nil {
			return err
		}

		ds = dsmount.New([]dsmount.Mount{
			{
				Prefix:    bstore.BlockPrefix,
				Datastore: ds,
			},
		})

		bs := bstore.NewBlockstore(ds.(*dsmount.Datastore))

		return dataset.Import(ctx, targetPath, bs, merkledag.V0CidPrefix(), parallel, batchReadNum)
	},
}

var addCmd = &cli.Command{
	Name:  "add",
	Usage: "import single file to ds-cluster",
	Action: func(c *cli.Context) error {
		confPath := c.String("conf")
		confPath, err := homedir.Expand(confPath)
		if err != nil {
			return err
		}
		err = os.MkdirAll(confPath, 0755)
		if err != nil {
			return err
		}

		cfg, err := config.ReadConfig(path.Join(confPath, config.DefaultConfigJson))
		if err != nil {
			return err
		}

		target := c.Args().First()
		target, err = homedir.Expand(target)
		if err != nil {
			return err
		}
		var ds ds.Datastore
		ds, err = clusterclient.NewClusterClient(context.Background(), cfg)
		if err != nil {
			return err
		}
		ds = dsmount.New([]dsmount.Mount{
			{
				Prefix:    bstore.BlockPrefix,
				Datastore: ds,
			},
		})
		bs2 := bstore.NewBlockstore(ds.(*dsmount.Datastore))
		dagServ := merkledag.NewDAGService(blockservice.New(bs2, offline.Exchange(bs2)))

		// cidbuilder
		cidBuilder, err := merkledag.PrefixForCidVersion(0)
		if err != nil {
			return err
		}

		files := filehelper.FileWalkAsync([]string{target})

		for item := range files {
			fileNode, err := filehelper.BuildFileNode(item, dagServ, cidBuilder)
			if err != nil {
				return err
			}
			k := dshelp.MultihashToDsKey(fileNode.Cid().Hash())
			logging.Infof("imported file: %s, root: %s, key: %s", item.Path, fileNode, k)
		}

		return nil
	},
}

var hashslotCmd = &cli.Command{
	Name:  "hashslot",
	Usage: "",
	Action: func(c *cli.Context) error {
		confPath := c.String("conf")
		confPath, err := homedir.Expand(confPath)
		if err != nil {
			return err
		}
		err = os.MkdirAll(confPath, 0755)
		if err != nil {
			return err
		}

		cfg, err := config.ReadConfig(path.Join(confPath, config.DefaultConfigJson))
		if err != nil {
			return err
		}

		target := c.Args().First()
		tcid, err := cid.Decode(target)
		if err != nil {
			return err
		}

		k := dshelp.MultihashToDsKey(tcid.Hash())
		fmt.Printf("ds key: %s\n", k)
		ctx := context.Background()

		ds, err := clusterclient.NewClusterClient(ctx, cfg)
		if err != nil {
			return err
		}
		n, err := ds.HashSlots(k)
		if err != nil {
			return err
		}
		fmt.Printf("id: %s\n", n.ID)
		fmt.Printf("slots start: %d\n", n.Slots.Start)
		fmt.Printf("slots end: %d\n", n.Slots.End)
		fmt.Printf("crc8code: %d\n", utils.CRC8code(k.String()))
		return nil
	},
}

var boundCmd = &cli.Command{
	Name:  "bound",
	Usage: "",
	Action: func(c *cli.Context) error {
		ctx := context.Background()
		confPath := c.String("conf")
		confPath, err := homedir.Expand(confPath)
		if err != nil {
			return err
		}
		err = os.MkdirAll(confPath, 0755)
		if err != nil {
			return err
		}

		cfg, err := config.ReadConfig(path.Join(confPath, config.DefaultConfigJson))
		if err != nil {
			return err
		}

		var ds ds.Datastore
		ds, err = clusterclient.NewClusterClient(context.Background(), cfg)
		if err != nil {
			return err
		}
		ds = dsmount.New([]dsmount.Mount{
			{
				Prefix:    bstore.BlockPrefix,
				Datastore: ds,
			},
		})
		bs2 := bstore.NewBlockstore(ds.(*dsmount.Datastore))
		dagServ := merkledag.NewDAGService(blockservice.New(bs2, offline.Exchange(bs2)))

		target := c.Args().First()
		tcid, err := cid.Decode(target)
		if err != nil {
			return err
		}
		utils.HeadFileNode(ctx, tcid, dagServ, "", func(path string, cid cid.Cid, err error) {
			if err != nil {
				logging.Error(err)
				return
			}
			fmt.Printf("head: %s,%s\n", path, cid)
		})
		utils.TailFileNode(ctx, tcid, dagServ, "", func(path string, cid cid.Cid, err error) {
			if err != nil {
				logging.Error(err)
				return
			}
			fmt.Printf("tail: %s,%s\n", path, cid)
		})

		return nil
	},
}

var statCmd = &cli.Command{
	Name:  "stat",
	Usage: "",
	Action: func(c *cli.Context) error {
		confPath := c.String("conf")
		confPath, err := homedir.Expand(confPath)
		if err != nil {
			return err
		}
		err = os.MkdirAll(confPath, 0755)
		if err != nil {
			return err
		}

		cfg, err := config.ReadConfig(path.Join(confPath, config.DefaultConfigJson))
		if err != nil {
			return err
		}

		target := c.Args().First()
		tcid, err := cid.Decode(target)
		if err != nil {
			return err
		}
		k := dshelp.MultihashToDsKey(tcid.Hash())
		fmt.Printf("ds key: %s\n", k)
		ctx := context.Background()
		var ds ds.Datastore
		ds, err = clusterclient.NewClusterClient(context.Background(), cfg)
		if err != nil {
			return err
		}
		ds = dsmount.New([]dsmount.Mount{
			{
				Prefix:    bstore.BlockPrefix,
				Datastore: ds,
			},
		})
		bs2 := bstore.NewBlockstore(ds.(*dsmount.Datastore))
		dagServ := merkledag.NewDAGService(blockservice.New(bs2, offline.Exchange(bs2)))

		dagNode, err := dagServ.Get(ctx, tcid)

		if err != nil {
			return err
		}

		stat, err := dagNode.Stat()
		if err != nil {
			return err
		}
		logging.Infof("%#v", stat)
		return nil
	},
}

var getCmd = &cli.Command{
	Name:  "get",
	Usage: "",
	Action: func(c *cli.Context) error {
		confPath := c.String("conf")
		confPath, err := homedir.Expand(confPath)
		if err != nil {
			return err
		}
		err = os.MkdirAll(confPath, 0755)
		if err != nil {
			return err
		}

		cfg, err := config.ReadConfig(path.Join(confPath, config.DefaultConfigJson))
		if err != nil {
			return err
		}

		args := c.Args().Slice()
		tcid, err := cid.Decode(args[0])
		if err != nil {
			return err
		}
		targetPath := args[1]
		if targetPath == "" {
			targetPath = args[0]
		}
		k := dshelp.MultihashToDsKey(tcid.Hash())
		logging.Infof("ds key: %s", k)
		ctx := context.Background()
		var ds ds.Datastore
		ds, err = clusterclient.NewClusterClient(context.Background(), cfg)
		if err != nil {
			return err
		}
		ds = dsmount.New([]dsmount.Mount{
			{
				Prefix:    bstore.BlockPrefix,
				Datastore: ds,
			},
		})
		bs2 := bstore.NewBlockstore(ds.(*dsmount.Datastore))
		dagServ := merkledag.NewDAGService(blockservice.New(bs2, offline.Exchange(bs2)))

		dagNode, err := dagServ.Get(ctx, tcid)

		if err != nil {
			return err
		}
		fdr, err := ufsio.NewDagReader(ctx, dagNode, dagServ)
		if err != nil {
			return err
		}
		f, err := os.Create(targetPath)
		if err != nil {
			return err
		}
		_, err = io.Copy(f, fdr)
		if err != nil {
			return err
		}

		return nil
	},
}

var initCmd = &cli.Command{
	Name: "init",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "bootstrapper",
			Usage: "",
		},
	},
	Usage: "",
	Action: func(c *cli.Context) error {
		confPath := c.String("conf")
		confPath, err := homedir.Expand(confPath)
		if err != nil {
			return err
		}
		err = os.MkdirAll(confPath, 0755)
		if err != nil {
			return err
		}

		cfg, err := config.ReadConfig(path.Join(confPath, config.DefaultConfigJson))
		if err != nil {
			cfg, err = config.GenClientConf()
			if err != nil {
				return err
			}
		}
		ctxbg := context.Background()
		bootstrapper := c.String("bootstrapper")
		if len(cfg.Nodes) == 0 {
			if bootstrapper == "" {
				return xerrors.Errorf("missing cluster nodes config info")
			}
			err = initClientConfig(ctxbg, cfg, confPath, bootstrapper)
			if err != nil {
				logging.Error(err)
				return err
			}
		}
		bs, err := json.MarshalIndent(cfg, "", "\t")
		if err != nil {
			return err
		}
		logging.Infof("%s", bs)
		return nil
	},
}

func initClientConfig(ctxbg context.Context, cfg *config.Config, confPath, bootstrapper string) (err error) {
	h1, err := p2p.MakeBasicHost(utils.RandPort())
	if err != nil {
		return
	}
	defer h1.Close()
	baddr, err := ma.NewMultiaddr(bootstrapper)
	if err != nil {
		return
	}
	pinfo, err := peer.AddrInfoFromP2pAddr(baddr)
	if err != nil {
		return
	}

	client := share.NewShareClient(ctxbg, h1, *pinfo)

	bs, err := client.GetClusterInfo()
	if err != nil {
		return
	}
	nodes := make([]config.Node, 0)
	err = json.Unmarshal(bs, &nodes)
	if err != nil {
		return
	}
	cfg.Nodes = nodes
	cfgbs, err := json.MarshalIndent(cfg, "", "\t")
	if err != nil {
		return
	}
	err = ioutil.WriteFile(path.Join(confPath, config.DefaultConfigJson), cfgbs, 0644)
	if err != nil {
		return
	}
	return
}
