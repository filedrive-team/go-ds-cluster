package mongods

import (
	"context"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	log "github.com/ipfs/go-log/v2"
	"go.mongodb.org/mongo-driver/mongo"
)

var logging = log.Logger("mongods")
var _ ds.Batching = (*MongoDS)(nil)

type MongoDS struct {
	ctx      context.Context
	dbclient *dbclient
}

func NewMongoDS(ctx context.Context, cfg *Config) (*MongoDS, error) {
	client, err := newDBClient(cfg)
	if err != nil {
		return nil, err
	}
	return &MongoDS{
		ctx:      ctx,
		dbclient: client,
	}, nil
}

func (m *MongoDS) Put(k ds.Key, value []byte) error {
	return m.dbclient.put(m.ctx, k, value)
}

func (m *MongoDS) Get(k ds.Key) ([]byte, error) {
	v, err := m.dbclient.get(m.ctx, k)
	if err == mongo.ErrNoDocuments {
		return v, ds.ErrNotFound
	}
	return v, err
}

func (m *MongoDS) Has(k ds.Key) (bool, error) {
	has, err := m.dbclient.has(m.ctx, k)
	if err == nil && !has {
		return has, ds.ErrNotFound
	}
	return has, err
}

func (m *MongoDS) GetSize(k ds.Key) (int, error) {
	size, err := m.dbclient.getSize(m.ctx, k)
	if err == mongo.ErrNoDocuments {
		return -1, ds.ErrNotFound
	}
	return size, err
}

func (m *MongoDS) Delete(k ds.Key) error {
	err := m.dbclient.delete(m.ctx, k)
	if err == mongo.ErrNoDocuments {
		return nil
	}
	return err
}

func (m *MongoDS) Sync(ds.Key) error {
	return nil
}

func (m *MongoDS) Close() error {
	return m.dbclient.close()
}

func (m *MongoDS) Query(q dsq.Query) (dsq.Results, error) {
	ent, errChan, err := m.dbclient.query(m.ctx, q)
	if err != nil {
		return nil, err
	}
	nextValue := func() (dsq.Result, bool) {
		select {
		case item, ok := <-ent:
			if !ok {
				return dsq.Result{}, false
			}
			return dsq.Result{Entry: *item}, true
		case err := <-errChan:
			return dsq.Result{Error: err}, false
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

func (m *MongoDS) Batch() (ds.Batch, error) {
	return ds.NewBasicBatch(m), nil
}
