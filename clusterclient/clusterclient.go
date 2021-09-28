package clusterclient

import (
	context "context"
	"sync"
	"sync/atomic"

	"github.com/filedrive-team/go-ds-cluster/config"
	"github.com/filedrive-team/go-ds-cluster/core"
	"github.com/filedrive-team/go-ds-cluster/p2p/store"
	"github.com/filedrive-team/go-ds-cluster/shard"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	log "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	ma "github.com/multiformats/go-multiaddr"
	"golang.org/x/xerrors"
)

var logging = log.Logger("clusterclient")
var _ ds.Batching = (*ClusterClient)(nil)

type ClusterClient struct {
	ctx     context.Context
	sm      *shard.SlotsManager
	nodeMap map[string]core.DataNodeClient
	host    host.Host
}

func NewClusterClient(ctx context.Context, cfg *config.Config) (*ClusterClient, error) {
	h, err := store.HostFromConf(cfg)
	if err != nil {
		return nil, err
	}
	// protect the connection with server node
	cm := h.ConnManager()
	for _, nd := range cfg.Nodes {
		pid, err := peer.Decode(nd.ID)
		if err != nil {
			return nil, err
		}
		cm.Protect(pid, "cluster-node")
	}
	sm, err := shard.RestoreSlotsManager(shardNodes(cfg.Nodes))
	if err != nil {
		return nil, err
	}
	nodeMap, err := makeNodeMap(ctx, h, cfg)
	if err != nil {
		return nil, err
	}
	return &ClusterClient{
		sm:      sm,
		ctx:     ctx,
		host:    h,
		nodeMap: nodeMap,
	}, nil
}

func (d *ClusterClient) nodeByKey(kstr string) (core.DataNodeClient, error) {
	sn, err := d.sm.NodeByKey(kstr)
	if err != nil {
		return nil, err
	}
	client, ok := d.nodeMap[sn.ID]
	if !ok {
		return nil, xerrors.Errorf("can not find DataNodeClient by: %s", sn.ID)
	}
	return client, nil
}

func (d *ClusterClient) Put(k ds.Key, value []byte) error {
	kstr := k.String()
	client, err := d.nodeByKey(kstr)
	if err != nil {
		return err
	}
	return client.Put(kstr, value)
}

func (d *ClusterClient) Get(k ds.Key) ([]byte, error) {
	kstr := k.String()
	client, err := d.nodeByKey(kstr)
	if err != nil {
		return nil, err
	}
	return client.Get(kstr)
}

func (d *ClusterClient) Has(k ds.Key) (bool, error) {
	kstr := k.String()
	client, err := d.nodeByKey(kstr)
	if err != nil {
		return false, err
	}
	return client.Has(kstr)
}

func (d *ClusterClient) GetSize(k ds.Key) (int, error) {
	kstr := k.String()
	client, err := d.nodeByKey(kstr)
	if err != nil {
		return -1, err
	}
	return client.GetSize(kstr)
}

func (d *ClusterClient) Delete(k ds.Key) error {
	kstr := k.String()
	client, err := d.nodeByKey(kstr)
	if err != nil {
		return err
	}
	return client.Delete(kstr)
}

func (d *ClusterClient) Sync(ds.Key) error {
	return nil
}

func (d *ClusterClient) Close() error {
	return d.host.Close()
}

func (d *ClusterClient) Query(q dsq.Query) (dsq.Results, error) {
	out := make(chan dsq.Result)
	stop := make(chan struct{})
	closeStop := func() {
		close(stop)
	}
	closeOut := func() {
		close(out)
	}
	var stopOnce sync.Once
	var outOnce sync.Once

	// figure out when to close all the channel
	cc := make(chan struct{})
	var closeCount int64
	go func(stop chan struct{}, cc chan struct{}) {
		for {
			select {
			case <-stop:
				outOnce.Do(closeOut)
				return
			case <-cc:
				atomic.AddInt64(&closeCount, 1)
				if atomic.LoadInt64(&closeCount) >= int64(len(d.nodeMap)) {
					stopOnce.Do(closeStop)
				}
			}
		}
	}(stop, cc)

	for _, dc := range d.nodeMap {
		go func(dc core.DataNodeClient, q dsq.Query, ch chan dsq.Result, stop chan struct{}, cc chan struct{}) {
			defer func() {
				cc <- struct{}{}
			}()
			results, err := dc.Query(q)
			if err != nil {
				logging.Error(err)
				return
			}
			for {
				select {
				case <-stop:
					results.Close()
					return
				case result, ok := <-results.Next():
					if !ok {
						return
					}
					out <- result
				}
			}

		}(dc, q, out, stop, cc)
	}

	nextValue := func() (dsq.Result, bool) {
		result, ok := <-out
		if !ok {
			return dsq.Result{}, false
		}

		if result.Error != nil {
			return result, false
		}

		return result, true
	}

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Close: func() error {
			stopOnce.Do(closeStop)
			return nil
		},
		Next: nextValue,
	}), nil
}

func (d *ClusterClient) Batch() (ds.Batch, error) {
	return ds.NewBasicBatch(d), nil
}

func makeNodeMap(ctx context.Context, host host.Host, cfg *config.Config) (map[string]core.DataNodeClient, error) {
	res := make(map[string]core.DataNodeClient)
	for _, nd := range cfg.Nodes {
		pid, err := peer.Decode(nd.ID)
		if err != nil {
			return nil, err
		}
		addrs := make([]ma.Multiaddr, 0, len(nd.Swarm))
		for _, addr := range nd.Swarm {
			maddr, err := ma.NewMultiaddr(addr)
			if err != nil {
				return nil, err
			}
			addrs = append(addrs, maddr)
		}
		res[nd.ID] = store.NewStoreClient(ctx, host, peer.AddrInfo{
			ID:    pid,
			Addrs: addrs,
		}, store.PROTOCOL_V1)
	}
	return res, nil
}

func shardNodes(nds []config.Node) []shard.Node {
	res := make([]shard.Node, 0, len(nds))
	for _, nd := range nds {
		res = append(res, shard.Node{
			ID:    nd.ID,
			Slots: nd.Slots,
		})
	}
	return res
}
