package mongods

import (
	"context"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	log "github.com/ipfs/go-log/v2"
)

var logging = log.Logger("mongods")
var _ ds.Batching = (*MongoDS)(nil)

type MongoDS struct {
	ctx context.Context
}

func (m *MongoDS) Put(k ds.Key, value []byte) error {
	return nil
}

func (m *MongoDS) Get(k ds.Key) ([]byte, error) {
	return nil, nil
}

func (m *MongoDS) Has(k ds.Key) (bool, error) {
	return false, nil
}

func (m *MongoDS) GetSize(k ds.Key) (int, error) {
	return 0, nil
}

func (m *MongoDS) Delete(k ds.Key) error {
	return nil
}

func (m *MongoDS) Sync(ds.Key) error {
	return nil
}

func (m *MongoDS) Close() error {
	return nil
}

func (m *MongoDS) Query(q dsq.Query) (dsq.Results, error) {
	return nil, nil
}

func (m *MongoDS) Batch() (ds.Batch, error) {
	return ds.NewBasicBatch(m), nil
}
