package mongods

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	db_uri          = "mongodb://localhost:27017"
	db_name         = "mongods"
	block_coll_name = "blocks"
	ref_coll_name   = "blocks_ref"
)

type Config struct {
	Uri           string `json:"uri"`
	DBName        string `json:"db_name"`
	BlockCollName string `json:"block_coll_name"`
	RefCollName   string `json:"ref_coll_name"`
}

func DefaultConf() *Config {
	return &Config{
		Uri:           db_uri,
		DBName:        db_name,
		BlockCollName: block_coll_name,
		RefCollName:   ref_coll_name,
	}
}

type Block struct {
	ID        string    `bson:"_id" json:"_id"`     // sha256 hash of Value
	Value     []byte    `bson:"value" json:"value"` // value
	CreatedAt time.Time `bson:"created_at" json:"created_at"`
}

type BlockRef struct {
	ID        string    `bson:"_id" json:"_id"` // key
	Ref       string    `bson:"ref" json:"ref"` // sha256 hash of Value
	Size      int       `bson:"size" json:"size"`
	CreatedAt time.Time `bson:"created_at" json:"created_at"`
}

func newDBClient(cfg *Config) (*dbclient, error) {
	var err error
	mgoClient, err := mongo.NewClient(options.Client().ApplyURI(cfg.Uri))
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = mgoClient.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &dbclient{
		client: mgoClient,
		cfg:    cfg,
	}, nil
}

type dbclient struct {
	client *mongo.Client
	cfg    *Config
}

func (db *dbclient) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return db.client.Disconnect(ctx)
}

func (db *dbclient) blockColl() *mongo.Collection {
	return db.client.Database(db.cfg.DBName).Collection(db.cfg.BlockCollName)
}

func (db *dbclient) refColl() *mongo.Collection {
	return db.client.Database(db.cfg.DBName).Collection(db.cfg.RefCollName)
}
