package remoteclient

import (
	context "context"

	"github.com/filedrive-team/go-ds-cluster/core"
	"github.com/filedrive-team/go-ds-cluster/p2p/remoteds"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	log "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
)

var logging = log.Logger("remoteclient")
var _ ds.Batching = (*RemoteStore)(nil)

type RemoteStore struct {
	ctx  context.Context
	node core.RemoteDataNodeClient
	host host.Host
}

func NewRemoteStore(ctx context.Context, h host.Host, target string, timeout int, token string) (*RemoteStore, error) {
	rc, err := MakeRemoteDataNodeClient(ctx, h, target, timeout, token)
	if err != nil {
		return nil, err
	}
	return &RemoteStore{
		ctx:  ctx,
		host: h,
		node: rc,
	}, nil
}

func NewRemoteStoreWithClient(ctx context.Context, h host.Host, client core.RemoteDataNodeClient) *RemoteStore {
	return &RemoteStore{
		ctx:  ctx,
		host: h,
		node: client,
	}
}

func (d *RemoteStore) Put(k ds.Key, value []byte) error {
	kstr := k.String()

	return d.node.Put(kstr, value)
}

func (d *RemoteStore) Get(k ds.Key) ([]byte, error) {
	kstr := k.String()

	return d.node.Get(kstr)
}

func (d *RemoteStore) Has(k ds.Key) (bool, error) {
	kstr := k.String()

	return d.node.Has(kstr)
}

func (d *RemoteStore) GetSize(k ds.Key) (int, error) {
	kstr := k.String()

	return d.node.GetSize(kstr)
}

func (d *RemoteStore) Delete(k ds.Key) error {
	kstr := k.String()

	return d.node.Delete(kstr)
}

func (d *RemoteStore) Sync(ds.Key) error {
	return nil
}

func (d *RemoteStore) Close() error {
	return d.host.Close()
}

func (d *RemoteStore) Query(q dsq.Query) (dsq.Results, error) {
	return d.node.Query(q)
}

type batch struct {
	s ds.Datastore
}

func (d *RemoteStore) Batch() (ds.Batch, error) {
	return &batch{d}, nil
}

func (b *batch) Put(key ds.Key, value []byte) error {
	return b.s.Put(key, value)
}

func (b *batch) Delete(key ds.Key) error {
	return b.s.Delete(key)
}

func (b *batch) Commit() error {
	return nil
}

func MakeRemoteDataNodeClient(ctx context.Context, host host.Host, target string, timeout int, token string) (core.RemoteDataNodeClient, error) {
	pinfo, err := peer.AddrInfoFromString(target)
	if err != nil {
		return nil, err
	}
	return remoteds.NewStoreClient(ctx, host, *pinfo, remoteds.PROTOCOL_V1, timeout, token), nil
}
