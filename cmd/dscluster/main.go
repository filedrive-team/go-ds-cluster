package main

import (
	"context"
	"flag"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/filedrive-team/go-ds-cluster/config"
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

func init() {
	log.SetLogLevelRegex("*", "info")
}

func main() {
	flag.StringVar(&confpath, "conf", ".dscluster", "")
	flag.Parse()

	confOption, err := config.LoadConfig(confpath)
	if err != nil {
		logging.Fatal(err)
	}
	app := fx.New(
		confOption,
		fx.Provide(
			BasicHost,
			ProtocolID,
			FlatFS,
		),
		fx.Invoke(Kickoff),
	)
	ctxbg := context.Background()

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

func Kickoff(lc fx.Lifecycle, h host.Host, pid protocol.ID, ds ds.Datastore) {
	ctx, cancel := context.WithCancel(context.Background())
	server := store.NewStoreServer(ctx, h, pid, ds)

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			cancel()
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
	h, err := store.HostFromConf(cfg)
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
