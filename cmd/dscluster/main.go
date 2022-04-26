package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/filedrive-team/go-ds-cluster/config"
	"github.com/filedrive-team/go-ds-cluster/mutcaskds"
	"github.com/filedrive-team/go-ds-cluster/p2p"
	"github.com/filedrive-team/go-ds-cluster/p2p/share"
	"github.com/filedrive-team/go-ds-cluster/p2p/store"
	"github.com/filedrive-team/go-ds-cluster/utils"
	ds "github.com/ipfs/go-datastore"
	log "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"github.com/mitchellh/go-homedir"
	ma "github.com/multiformats/go-multiaddr"
	"go.uber.org/fx"
)

var logging = log.Logger("dscluster")
var confpath string
var mutcask string
var loglevel string
var disableDelete string
var identityIdx int
var bootstrapper string

func main() {
	flag.StringVar(&confpath, "conf", config.DefaultConfigPath, "")
	flag.StringVar(&mutcask, "mutcask", "", "")
	flag.StringVar(&loglevel, "log-level", "error", "")
	flag.StringVar(&disableDelete, "disable-delete", "", "")
	flag.IntVar(&identityIdx, "identity", 0, "get node identity from bootstrap node")
	flag.StringVar(&bootstrapper, "bootstrapper", "", "")
	flag.Parse()
	log.SetLogLevel("*", loglevel)
	var disabledel bool
	if disableDelete == "true" {
		disabledel = true
	}

	ctxbg := context.Background()
	ctxOption := fx.Provide(func() context.Context {
		return ctxbg
	})
	confpath, err := homedir.Expand(confpath)
	if err != nil {
		logging.Error(err)
		return
	}
	logging.Info(confpath)
	err = os.MkdirAll(confpath, 0755)
	if err != nil {
		logging.Error(err)
		return
	}
	cfg, err := config.ReadConfig(path.Join(confpath, config.DefaultConfigJson))
	if err != nil {
		// other nodes get identity and cluster nodes info from bootstrap node
		if identityIdx > 0 && bootstrapper != "" {
			cfg, err = initClusterConfig(ctxbg, confpath, bootstrapper, disabledel)
			if err != nil {
				logging.Error(err)
				return
			}
		} else {
			logging.Error(err)
			return
		}
	}
	// bootstrap node should hold the cluster nodes info
	if cfg.BootstrapNode && (len(cfg.Nodes) == 0 || len(cfg.IdentityList) == 0) {
		logging.Error("Bootstrap node but doesn't hold cluster nodes info or doesn't hold indentity info")
		return
	}
	// todo:
	// currently use idx 0 identity as bootstrap node
	// need a way make it more flexible
	if cfg.BootstrapNode && cfg.Identity.PeerID != cfg.IdentityList[0].PeerID {
		logging.Error("Bootstrap node identity not match")
		return
	}

	cfg.ConfPath = confpath
	if disableDelete == "true" {
		cfg.DisableDelete = true
	} else if disableDelete == "false" {
		cfg.DisableDelete = false
	}
	confOption := fx.Provide(func() *config.Config {
		return cfg
	})

	dsOption := fx.Provide(func(ctx context.Context, lc fx.Lifecycle, cfg *config.Config) (ds.Datastore, error) {
		// load customized mutcask configs
		conf, err := loadMutcaskConf(cfg, mutcask)
		if err != nil {
			return nil, err
		}
		mutds, err := mutcaskds.NewMutcaskDS(ctx, conf)
		if err != nil {
			return nil, err
		}
		lc.Append(fx.Hook{
			OnStop: func(ctx context.Context) error {
				return mutds.Close()
			},
		})
		return mutds, nil
	})

	app := fx.New(
		ctxOption,
		confOption,
		dsOption,
		fx.Provide(
			BasicHost,
			ProtocolID,
		),
		fx.Invoke(Kickoff),
	)

	startctx, cancel := context.WithTimeout(ctxbg, 5*time.Second)
	defer cancel()
	if err := app.Start(startctx); err != nil {
		logging.Fatal(err)
	}

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logging.Info("Shutdown Server ...")

	stopctx, cancel := context.WithTimeout(ctxbg, 5*time.Second)
	defer cancel()
	if err := app.Stop(stopctx); err != nil {
		logging.Fatal(err)
	}
}

func Kickoff(lc fx.Lifecycle, h host.Host, pid protocol.ID, ds ds.Datastore, cfg *config.Config) {
	ctx, cancel := context.WithCancel(context.Background())
	server := store.NewStoreServer(ctx, h, pid, ds, cfg.DisableDelete)
	var shareSrv *share.Server
	if cfg.BootstrapNode {
		shareSrv = share.NewShareServer(ctx, h, cfg)
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) (err error) {
			defer cancel()
			if cfg.BootstrapNode {
				err = shareSrv.Close()
			}
			err = server.Close()
			return
		},
		OnStart: func(ctx context.Context) error {
			server.Serve()
			if cfg.BootstrapNode {
				shareSrv.Serve()
			}
			return nil
		},
	})
}

func ProtocolID() protocol.ID {
	return store.PROTOCOL_V1
}

func BasicHost(lc fx.Lifecycle, cfg *config.Config) (host.Host, error) {
	h, err := p2p.HostFromConf(cfg)
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return h.Close()
		},
	})

	return h, nil
}

func initClusterConfig(ctxbg context.Context, confpath, bootstrapper string, disabledel bool) (cfg *config.Config, err error) {
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

	// test get identity
	bs, err := client.GetIdentity(identityIdx)
	if err != nil {
		return
	}
	ident := config.Identity{}
	err = json.Unmarshal(bs, &ident)
	if err != nil {
		return
	}
	bs, err = client.GetClusterInfo()
	if err != nil {
		return
	}
	nodes := make([]config.Node, 0)
	err = json.Unmarshal(bs, &nodes)
	if err != nil {
		return
	}
	cfg = &config.Config{
		Identity: ident,
		Nodes:    nodes,
		Addresses: config.Addresses{
			Swarm: nodes[identityIdx].Swarm,
		},
		DisableDelete: disabledel,
	}
	cfgbs, err := json.MarshalIndent(cfg, "", "\t")
	if err != nil {
		return
	}
	err = ioutil.WriteFile(path.Join(confpath, config.DefaultConfigJson), cfgbs, 0644)
	if err != nil {
		return
	}
	return
}

func loadMutcaskConf(cfg *config.Config, mutcaskConf string) (conf *mutcaskds.Config, err error) {
	conf = &mutcaskds.Config{
		Path:            cfg.Mutcask.Path,
		CaskNum:         cfg.Mutcask.CaskNum,
		HintBootReadNum: cfg.Mutcask.HintBootReadNum,
	}
	if mutcaskConf != "" {
		cfgpath := mutcaskConf
		if !strings.HasPrefix(cfgpath, "/") {
			cfgpath = filepath.Join(cfg.ConfPath, cfgpath)
		}
		conf, err = mutcaskds.LoadConfig(cfgpath)
		if err != nil {
			return nil, err
		}
	}
	if conf.Path == "" {
		conf.Path = config.DefaultMutcaskPath
	}
	if conf.CaskNum == 0 {
		conf.CaskNum = config.DefaultCaskNum
	}
	if !strings.HasPrefix(conf.Path, "/") {
		conf.Path = filepath.Join(cfg.ConfPath, conf.Path)
	}
	return
}
