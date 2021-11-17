package miniods

import (
	"bytes"
	"context"
	"io/ioutil"
	"path/filepath"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	log "github.com/ipfs/go-log/v2"
	minio "github.com/minio/minio-go/v6"
	"golang.org/x/xerrors"
)

const StatusNotFound = 404

var logging = log.Logger("miniods")
var _ ds.Datastore = (*MinioDS)(nil)

type Config struct {
	Endpoint  string
	AccessKey string
	SecretKey string
	Bucket    string
	Region    string
	Root      string
	Ssl       bool
}

type MinioDS struct {
	ctx    context.Context
	client *minio.Client
	cfg    *Config
}

func NewMinioDS(ctx context.Context, cfg *Config) (*MinioDS, error) {
	client, err := minio.NewWithRegion(cfg.Endpoint, cfg.AccessKey, cfg.SecretKey, cfg.Ssl, cfg.Region)
	if err != nil {
		return nil, err
	}
	// check if bucket exists
	exists, err := client.BucketExists(cfg.Bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		// create bucket
		if err := client.MakeBucket(cfg.Bucket, cfg.Region); err != nil {
			return nil, err
		}
	}
	return &MinioDS{
		ctx:    ctx,
		client: client,
		cfg:    cfg,
	}, nil
}

func (m *MinioDS) Put(k ds.Key, value []byte) error {
	fname := filepath.Join(m.cfg.Root, k.String())
	fsize := int64(len(value))
	_, err := m.client.PutObjectWithContext(m.ctx, m.cfg.Bucket, fname, bytes.NewReader(value), fsize, minio.PutObjectOptions{})
	return err
}

func (m *MinioDS) Get(k ds.Key) ([]byte, error) {
	fname := filepath.Join(m.cfg.Root, k.String())
	res, err := m.client.GetObjectWithContext(m.ctx, m.cfg.Bucket, fname, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer res.Close()

	v, err := ioutil.ReadAll(res)
	if err != nil {
		if e, ok := err.(minio.ErrorResponse); ok && e.StatusCode == StatusNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, err
	}
	return v, nil
}

func (m *MinioDS) Has(k ds.Key) (bool, error) {
	fname := filepath.Join(m.cfg.Root, k.String())
	_, err := m.client.StatObjectWithContext(m.ctx, m.cfg.Bucket, fname, minio.StatObjectOptions{})
	if err != nil {
		if e, ok := err.(minio.ErrorResponse); ok && e.StatusCode == StatusNotFound {
			return false, ds.ErrNotFound
		}
		return false, err
	}
	return true, nil
}

func (m *MinioDS) GetSize(k ds.Key) (int, error) {
	fname := filepath.Join(m.cfg.Root, k.String())
	info, err := m.client.StatObjectWithContext(m.ctx, m.cfg.Bucket, fname, minio.StatObjectOptions{})
	if err != nil {
		if e, ok := err.(minio.ErrorResponse); ok && e.StatusCode == StatusNotFound {
			return -1, ds.ErrNotFound
		}
		return -1, err
	}
	return int(info.Size), nil
}

func (m *MinioDS) Delete(k ds.Key) error {
	fname := filepath.Join(m.cfg.Root, k.String())
	return m.client.RemoveObject(m.cfg.Bucket, fname)
}

func (m *MinioDS) Sync(ds.Key) error {
	return nil
}

func (m *MinioDS) Close() error {
	return nil
}

func (m *MinioDS) Query(q dsq.Query) (dsq.Results, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, xerrors.New("miniods: orders or filters are not supported")
	}
	donechan := make(chan struct{})
	prefix := filepath.Join(m.cfg.Root, q.Prefix)
	oinfo := m.client.ListObjectsV2(m.cfg.Bucket, prefix, true, donechan)

	nextValue := func() (dsq.Result, bool) {
		select {
		case obj, ok := <-oinfo:
			if !ok {
				return dsq.Result{}, false
			}
			if obj.Err != nil {
				return dsq.Result{Error: obj.Err}, false
			}
			return dsq.Result{Entry: dsq.Entry{
				Key:  obj.Key,
				Size: int(obj.Size),
			}}, true
		case <-m.ctx.Done():
			return dsq.Result{Error: m.ctx.Err()}, false
		}
	}

	// Todo
	// implement Close method rather than return nil
	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Close: func() error {
			close(donechan)
			return nil
		},
		Next: nextValue,
	}), nil
}
