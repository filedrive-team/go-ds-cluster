package remoteclient

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/filedrive-team/filehelper/importer"
	"github.com/filedrive-team/go-ds-cluster/core"
	"github.com/filedrive-team/go-ds-cluster/p2p/remoteds"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
)

type MetaInfo struct {
	Path string
	Name string
	Size int64
	Cid  string
}

type Client struct {
	ctx     context.Context
	dagserv format.DAGService
	rdc     core.RemoteDataNodeClient
}

func NewClient(ctx context.Context, cfg *Config) (*Client, error) {
	h, err := hostForRemoteClient(cfg)
	if err != nil {
		return nil, err
	}
	rc, err := MakeRemoteDataNodeCleint(ctx, h, cfg.Target, cfg.Timeout, cfg.AccessToken)
	if err != nil {
		return nil, err
	}
	cds := NewRemoteStoreWithClient(ctx, h, rc)

	var blkst blockstore.Blockstore = blockstore.NewBlockstore(cds)

	dagServ := merkledag.NewDAGService(blockservice.New(blkst, offline.Exchange(blkst)))
	return &Client{
		ctx:     ctx,
		dagserv: dagServ,
		rdc:     rc,
	}, nil

}

func (cl *Client) Add(p string, objname string) (*MetaInfo, error) {
	cidBuilder, err := merkledag.PrefixForCidVersion(0)
	if err != nil {
		return nil, err
	}
	finfo, err := os.Stat(p)
	if err != nil {
		return nil, err
	}
	fsize := finfo.Size()
	f, err := os.Open(p)
	if err != nil {
		return nil, err
	}
	ndcid, err := importer.BalanceNode(cl.ctx, f, fsize, cl.dagserv, cidBuilder, 10)
	if err != nil {
		return nil, err
	}
	meta := &MetaInfo{
		Path: objname,
		Name: filepath.Base(p),
		Size: fsize,
		Cid:  ndcid.String(),
	}
	metabytes, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	err = cl.rdc.TouchFile(remotedsKey(objname), metabytes)
	if err != nil {
		return nil, err
	}

	return meta, nil
}

func remotedsKey(objname string) string {
	return datastore.NewKey(remoteds.PREFIX).Child(datastore.NewKey(objname)).String()
}
