package mongods

import (
	"bytes"
	"context"
	"path/filepath"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	log "github.com/ipfs/go-log/v2"
	minio "github.com/minio/minio-go/v6"
)

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
	return nil, nil
}

func (m *MinioDS) Has(k ds.Key) (bool, error) {
	return false, nil
}

func (m *MinioDS) GetSize(k ds.Key) (int, error) {
	return 0, nil
}

func (m *MinioDS) Delete(k ds.Key) error {
	return nil
}

func (m *MinioDS) Sync(ds.Key) error {
	return nil
}

func (m *MinioDS) Close() error {
	return nil
}

func (m *MinioDS) Query(q dsq.Query) (dsq.Results, error) {
	return nil, nil
}
