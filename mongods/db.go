package mongods

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/sigurn/crc8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/xerrors"
)

const (
	db_uri          = "mongodb://localhost:27017"
	db_name         = "mongods"
	db_test_name    = "mongods_test"
	block_coll_name = "blocks"
	ref_coll_name   = "blocks_ref"
)

type Config struct {
	Uri           string `json:"uri"`
	DBName        string `json:"db_name"`
	BlockCollName string `json:"block_coll_name"`
	RefCollName   string `json:"ref_coll_name"`
}

func ExtendConf(cfg *Config) *Config {
	res := DefaultConf()
	if cfg.Uri != "" {
		res.Uri = cfg.Uri
	}
	if cfg.DBName != "" {
		res.DBName = cfg.DBName
	}
	if cfg.BlockCollName != "" {
		res.BlockCollName = cfg.BlockCollName
	}
	if cfg.RefCollName != "" {
		res.RefCollName = cfg.RefCollName
	}
	return res
}

func DefaultConf() *Config {
	return &Config{
		Uri:           db_uri,
		DBName:        db_name,
		BlockCollName: block_coll_name,
		RefCollName:   ref_coll_name,
	}
}

// Block - binary data
type Block struct {
	ID        string    `bson:"_id" json:"_id"`     // sha256 hash of Value
	Value     []byte    `bson:"value" json:"value"` // value
	CreatedAt time.Time `bson:"created_at" json:"created_at"`
}

// BlockRef - reference for Block
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

func (db *dbclient) blockColl(code uint8) *mongo.Collection {
	return db.client.Database(db.cfg.DBName).Collection(fmt.Sprintf("%s_%d", db.cfg.BlockCollName, code))
}

func (db *dbclient) refColl(code uint8) *mongo.Collection {
	return db.client.Database(db.cfg.DBName).Collection(fmt.Sprintf("%s_%d", db.cfg.RefCollName, code))
}

func (db *dbclient) hasBlock(ctx context.Context, id string) (bool, error) {
	blockColl := db.blockColl(crc8code(id))
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
	refColl := db.refColl(crc8code(id))
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
	kstr := key.String()
	code := crc8code(kstr)
	blockColl := db.blockColl(code)
	refColl := db.refColl(code)

	blockID := sha256String(value)

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

func (db *dbclient) delete(ctx context.Context, key ds.Key) error {
	kstr := key.String()
	code := crc8code(kstr)
	blockColl := db.blockColl(code)
	refColl := db.refColl(code)

	var err error

	ref := &BlockRef{}
	if err = refColl.FindOne(ctx, bson.M{"_id": kstr}).Decode(ref); err != nil {
		return err
	}
	// delete record on ref collection
	if _, err = refColl.DeleteOne(ctx, bson.M{"_id": kstr}); err != nil {
		return err
	}
	// check if has other ref point to the same value
	rc, err := refColl.CountDocuments(ctx, bson.M{"ref": ref.Ref})
	if err != nil {
		return err
	}
	// delete record in block collection if rc is 0
	if rc < 1 {
		if _, err = blockColl.DeleteOne(ctx, bson.M{"_id": ref.Ref}); err != nil {
			return err
		}
	}

	return nil
}

func (db *dbclient) get(ctx context.Context, key ds.Key) ([]byte, error) {
	kstr := key.String()
	code := crc8code(kstr)
	blockColl := db.blockColl(code)
	refColl := db.refColl(code)

	ref := &BlockRef{}
	if err := refColl.FindOne(ctx, bson.M{"_id": key.String()}).Decode(ref); err != nil {
		return nil, err
	}
	block := &Block{}
	if err := blockColl.FindOne(ctx, bson.M{"_id": ref.Ref}).Decode(block); err != nil {
		return nil, err
	}
	return block.Value, nil
}

func (db *dbclient) has(ctx context.Context, key ds.Key) (bool, error) {
	return db.hasRef(ctx, key.String())
}

func (db *dbclient) getSize(ctx context.Context, key ds.Key) (int, error) {
	refColl := db.refColl(crc8code(key.String()))
	ref := &BlockRef{}
	if err := refColl.FindOne(ctx, bson.M{"_id": key.String()}).Decode(ref); err != nil {
		return -1, err
	}
	return ref.Size, nil
}

func (db *dbclient) query(ctx context.Context, q dsq.Query) (chan *dsq.Entry, chan error, error) {
	if q.Orders != nil || q.Filters != nil {
		return nil, nil, xerrors.Errorf("mongods currently not support orders or filters")
	}

	out := make(chan *dsq.Entry)
	errChan := make(chan error)
	go func() {
		defer close(out)

		mds := db.client.Database(db.cfg.DBName)
		colnames, err := mds.ListCollectionNames(ctx, bson.M{})
		if err != nil {
			errChan <- err
			return
		}
		cds := make([]string, 0)
		for _, colname := range colnames {
			if strings.HasPrefix(colname, db.cfg.RefCollName) {
				cds = append(cds, strings.TrimPrefix(colname, db.cfg.RefCollName))
			}
		}
		for _, cd := range cds {
			refColl := mds.Collection(db.cfg.RefCollName + cd)
			blockColl := mds.Collection(db.cfg.BlockCollName + cd)
			offset := int64(q.Offset)
			limit := int64(q.Limit)
			opts := options.FindOptions{}
			if offset > 0 {
				opts.Skip = &offset
			}
			if limit > 0 {
				opts.Limit = &limit
			}

			cur, err := refColl.Find(ctx, bson.M{
				"_id": primitive.Regex{
					Pattern: "^" + q.Prefix,
					Options: "i",
				},
			}, &opts)

			if err != nil {
				errChan <- err
				return
			}
			logging.Info("mongods query: got mongo cursor")
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if cur.Next(ctx) {
						ref := &BlockRef{}
						err := cur.Decode(ref)
						if err != nil {
							errChan <- err
							return
						}
						ent := &dsq.Entry{
							Key:  ref.ID,
							Size: int(ref.Size),
						}
						if !q.KeysOnly {
							b := &Block{}
							err = blockColl.FindOne(ctx, bson.M{"_id": ref.Ref}).Decode(b)
							if err != nil {
								errChan <- err
								return
							}
							ent.Value = b.Value
						}
						out <- ent
					} else {
						return
					}
				}
			}

		}
	}()
	return out, errChan, nil
}

func sha256String(d []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(d))
}

func crc8code(k string) uint8 {
	table := crc8.MakeTable(crc8.CRC8_MAXIM)
	return crc8.Checksum([]byte(k), table)
}
