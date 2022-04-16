package mutcaskds

import (
	"context"
	"encoding/json"
	"io/ioutil"

	kv "github.com/filedag-project/mutcask"
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
	kv, err := kv.NewMutcask(func(conf *kv.Config) {
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

func (dis *MutcaskDS) Put(ctx context.Context, k ds.Key, value []byte) error {
	return dis.kv.Put(k.String(), value)
}

func (dis *MutcaskDS) Get(ctx context.Context, k ds.Key) ([]byte, error) {
	v, err := dis.kv.Get(k.String())
	if err != nil {
		if err == kv.ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}
	return v, nil
}

func (dis *MutcaskDS) Has(ctx context.Context, k ds.Key) (bool, error) {
	_, err := dis.kv.Size(k.String())
	if err != nil {
		if err == kv.ErrNotFound {
			return false, ds.ErrNotFound
		}
		return false, err
	}
	return true, nil
}

func (dis *MutcaskDS) GetSize(ctx context.Context, k ds.Key) (int, error) {
	n, err := dis.kv.Size(k.String())
	if err != nil {
		if err == kv.ErrNotFound {
			return -1, ds.ErrNotFound
		}
		return -1, err
	}
	return n, nil
}

func (dis *MutcaskDS) Delete(ctx context.Context, k ds.Key) error {
	return dis.kv.Delete(k.String())
}

func (dis *MutcaskDS) Sync(context.Context, ds.Key) error {
	return nil
}

func (dis *MutcaskDS) Close() error {
	return dis.kv.Close()
}

func (dis *MutcaskDS) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
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
