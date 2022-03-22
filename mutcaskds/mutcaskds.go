package mutcaskds

import (
	"context"
	"encoding/json"
	"io/ioutil"

	"github.com/filedag-project/filedag-storage/kv"
	"github.com/filedag-project/filedag-storage/kv/mutcask"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"golang.org/x/xerrors"
)

var _ ds.Datastore = (*MutcaskDS)(nil)

type Config struct {
	Path    string `json:"path"`
	CaskNum uint32 `json:"cask_num"`
}

type MutcaskDS struct {
	ctx context.Context
	kv  kv.KVDB
}

func NewMutcaskDS(ctx context.Context, cfg *Config) (*MutcaskDS, error) {
	kv, err := mutcask.NewMutcask(func(conf *mutcask.Config) {
		if cfg.Path != "" {
			conf.Path = cfg.Path
		}
		if cfg.CaskNum > 0 {
			conf.CaskNum = cfg.CaskNum
		}

	})
	if err != nil {
		return nil, err
	}

	return &MutcaskDS{
		ctx: ctx,
		kv:  kv,
	}, nil
}

func (dis *MutcaskDS) Put(k ds.Key, value []byte) error {
	return dis.kv.Put(k.String(), value)
}

func (dis *MutcaskDS) Get(k ds.Key) ([]byte, error) {
	v, err := dis.kv.Get(k.String())
	if err != nil {
		if err == kv.ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}
	return v, nil
}

func (dis *MutcaskDS) Has(k ds.Key) (bool, error) {
	_, err := dis.kv.Size(k.String())
	if err != nil {
		if err == kv.ErrNotFound {
			return false, ds.ErrNotFound
		}
		return false, err
	}
	return true, nil
}

func (dis *MutcaskDS) GetSize(k ds.Key) (int, error) {
	n, err := dis.kv.Size(k.String())
	if err != nil {
		if err == kv.ErrNotFound {
			return -1, ds.ErrNotFound
		}
		return -1, err
	}
	return n, nil
}

func (dis *MutcaskDS) Delete(k ds.Key) error {
	return dis.kv.Delete(k.String())
}

func (dis *MutcaskDS) Sync(ds.Key) error {
	return nil
}

func (dis *MutcaskDS) Close() error {
	return dis.kv.Close()
}

func (dis *MutcaskDS) Query(q dsq.Query) (dsq.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, xerrors.New("MutcaskDS: orders or filters are not supported")
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
