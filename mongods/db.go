package mongods

import (
	"context"
	"crypto/sha256"
	"fmt"
	"time"

	ds "github.com/ipfs/go-datastore"
	"go.mongodb.org/mongo-driver/bson"
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

func (db *dbclient) hasBlock(ctx context.Context, id string) (bool, error) {
	blockColl := db.blockColl()
	err := blockColl.FindOne(ctx, bson.M{"_id": id}).Err()

	if err == nil {
		return true, nil
	}
	if err == mongo.ErrNoDocuments {
		return false, nil
	}
	return false, err
}

func (db *dbclient) hasRef(ctx context.Context, id string) (bool, error) {
	refColl := db.refColl()
	err := refColl.FindOne(ctx, bson.M{"_id": id}).Err()

	if err == nil {
		return true, nil
	}
	if err == mongo.ErrNoDocuments {
		return false, nil
	}
	return false, err
}

func (db *dbclient) put(ctx context.Context, key ds.Key, value []byte) error {
	blockColl := db.blockColl()
	refColl := db.refColl()

	blockID := sha256String(value)
	kstr := key.String()
	// check if has record in block collection
	hasBlock, err := db.hasBlock(ctx, blockID)
	if err != nil {
		return err
	}
	if !hasBlock {
		if _, err = blockColl.InsertOne(ctx, &Block{
			ID:        blockID,
			Value:     value,
			CreatedAt: time.Now(),
		}); err != nil {
			return err
		}
	}
	// check if has record in ref collection
	hasRef, err := db.hasRef(ctx, kstr)
	if err != nil {
		return err
	}
	if !hasRef {
		if _, err = refColl.InsertOne(ctx, &BlockRef{
			ID:        kstr,
			Ref:       blockID,
			Size:      len(value),
			CreatedAt: time.Now(),
		}); err != nil {
			return err
		}
	}
	return nil
}

func sha256String(d []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(d))
}
