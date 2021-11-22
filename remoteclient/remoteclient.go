package remoteclient

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/filedrive-team/filehelper/importer"
	"github.com/filedrive-team/go-ds-cluster/core"
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	ufsio "github.com/ipfs/go-unixfs/io"
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
	cfg     *Config
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
		cfg:     cfg,
	}, nil

}

func (cl *Client) Get(objname string) (io.ReadCloser, error) {
	meta, err := cl.FileInfo(objname)
	if err != nil {
		return nil, err
	}
	return cl.GetByCid(meta.Cid)
}

func (cl *Client) GetByCid(cidstr string) (io.ReadCloser, error) {
	cid, err := cid.Decode(cidstr)
	if err != nil {
		return nil, err
	}
	dagNode, err := cl.dagserv.Get(cl.ctx, cid)
	if err != nil {
		return nil, err
	}
	fdr, err := ufsio.NewDagReader(cl.ctx, dagNode, cl.dagserv)
	if err != nil {
		return nil, err
	}
	return fdr, nil
}

func (cl *Client) FileInfo(objname string) (*MetaInfo, error) {
	b, err := cl.rdc.FileInfo(filepath.Join(cl.cfg.Bucket, objname))
	if err != nil {
		return nil, err
	}
	res := new(MetaInfo)
	if err := json.Unmarshal(b, res); err != nil {
		return nil, err
	}
	return res, nil
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
	objname = filepath.Join(cl.cfg.Bucket, objname)
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
	err = cl.rdc.TouchFile(objname, metabytes)
	if err != nil {
		return nil, err
	}

	return meta, nil
}
