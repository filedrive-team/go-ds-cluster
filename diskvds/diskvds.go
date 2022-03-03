package diskvds

import (
	"context"
	"encoding/json"
	"io/ioutil"

	"github.com/filedag-project/filedag-storage/kv/diskv"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"golang.org/x/xerrors"
)

//var logging = log.Logger("diskvds")
var _ ds.Datastore = (*DiskvDS)(nil)

type Config struct {
	Dir            string
	MaxLinkDagSize int
	Parallel       int
	MaxCacheDags   int
}

type DiskvDS struct {
	ctx context.Context
	kv  *diskv.DisKV
}

func NewDiskvDS(ctx context.Context, cfg *Config) (*DiskvDS, error) {
	kv, err := diskv.NewDisKV(func(conf *diskv.Config) {
		if cfg.Dir != "" {
			conf.Dir = cfg.Dir
		}
		if cfg.MaxCacheDags > 0 {
			conf.MaxCacheDags = cfg.MaxCacheDags
		}
		if cfg.MaxLinkDagSize > 0 {
			conf.MaxLinkDagSize = cfg.MaxLinkDagSize
		}
		if cfg.Parallel > 0 {
			conf.Parallel = cfg.Parallel
		}
	})
	if err != nil {
		return nil, err
	}

	return &DiskvDS{
		ctx: ctx,
		kv:  kv,
	}, nil
}

func (dis *DiskvDS) Put(k ds.Key, value []byte) error {
	return dis.kv.Put(k.String(), value)
}

func (dis *DiskvDS) Get(k ds.Key) ([]byte, error) {
	v, err := dis.kv.Get(k.String())
	if err != nil {
		if err == diskv.ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}
	return v, nil
}

func (dis *DiskvDS) Has(k ds.Key) (bool, error) {
	_, err := dis.kv.Size(k.String())
	if err != nil {
		if err == diskv.ErrNotFound {
			return false, ds.ErrNotFound
		}
		return false, err
	}
	return true, nil
}

func (dis *DiskvDS) GetSize(k ds.Key) (int, error) {
	n, err := dis.kv.Size(k.String())
	if err != nil {
		if err == diskv.ErrNotFound {
			return -1, ds.ErrNotFound
		}
		return -1, err
	}
	return n, nil
}

func (dis *DiskvDS) Delete(k ds.Key) error {
	return dis.kv.Delete(k.String())
}

func (dis *DiskvDS) Sync(ds.Key) error {
	return nil
}

func (dis *DiskvDS) Close() error {
	return dis.kv.Close()
}

func (dis *DiskvDS) Query(q dsq.Query) (dsq.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, xerrors.New("Diskvds: orders or filters are not supported")
	}

	kc, err := dis.kv.AllKeysChan(dis.ctx)
	if err != nil {
		return nil, err
	}
	nextValue := func() (dsq.Result, bool) {
		select {
		case k, ok := <-kc:
			if !ok {
				return dsq.Result{}, false
			}

			return dsq.Result{Entry: dsq.Entry{
				Key: k,
			}}, true
		case <-dis.ctx.Done():
			return dsq.Result{Error: dis.ctx.Err()}, false
		}
	}

	// Todo
	// implement Close method rather than return nil
	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Close: func() error {
			return nil
		},
		Next: nextValue,
	}), nil
}

func LoadConfig(path string) (*Config, error) {
	cfg := new(Config)
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
