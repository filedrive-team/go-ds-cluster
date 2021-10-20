package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/filedrive-team/go-ds-cluster/config"
	"github.com/filedrive-team/go-ds-cluster/mongods"
	"github.com/filedrive-team/go-ds-cluster/p2p"
	"github.com/filedrive-team/go-ds-cluster/p2p/store"
	ds "github.com/ipfs/go-datastore"
	flatfs "github.com/ipfs/go-ds-flatfs"
	log "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/protocol"
	"go.uber.org/fx"
)

var logging = log.Logger("dscluster")
var confpath string
var mongodb string
var loglevel string
var disableDelete string

func main() {
	flag.StringVar(&confpath, "conf", ".dscluster", "")
	flag.StringVar(&mongodb, "mongodb", "", "")
	flag.StringVar(&loglevel, "loglevel", "error", "")
	flag.StringVar(&disableDelete, "disable-delete", "", "")
	flag.Parse()
	log.SetLogLevel("*", loglevel)

	ctxbg := context.Background()
	ctxOption := fx.Provide(func() context.Context {
		return ctxbg
	})
	cfg, err := config.ReadConfig(confpath + "/config.json")
	if err != nil {
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

	var dsOption fx.Option
	if mongodb != "" {
		dsOption = fx.Provide(func(ctx context.Context, lc fx.Lifecycle) (ds.Datastore, error) {
			monds, err := mongods.NewMongoDS(ctx, mongods.ExtendConf(&mongods.Config{
				Uri: mongodb,
			}))
			if err != nil {
				return nil, err
			}
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					return monds.Close()
				},
			})
			return monds, nil
		})
	} else {
		dsOption = fx.Provide(FlatFS)
	}

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

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			defer cancel()
			return server.Close()
		},
		OnStart: func(ctx context.Context) error {
			server.Serve()
			return nil
		},
	})
}

func FlatFS(cfg *config.Config) (ds.Datastore, error) {
	p := cfg.ConfPath + "/blocks"
	shardFunc, err := flatfs.ParseShardFunc("/repo/flatfs/shard/v1/next-to-last/2")
	if err != nil {
		return nil, err
	}
	return flatfs.CreateOrOpen(p, shardFunc, true)
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
