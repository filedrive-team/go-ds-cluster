package mongods

import (
	"bytes"
	"context"
	"testing"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
)

type Pair struct {
	Key   string
	Value []byte
}

var tdata = []Pair{
	{"Filedrive", []byte("Platform for better use of datasets on web3")},
	{"FileDAG", []byte("Destributed storage provider")},
	{"afsis", []byte("Africa Soil Information Service (AfSIS) Soil Chemistry")},
	{"tablestore", []byte("AI2 Tablestore (November 2015 Snapshot)")},
	{"tuple-kb", []byte("Aristo Tuple KB")},
	{"amazon-conversational-product-search", []byte("Voice-based refinements of product search")},
	{"Amazon-PQA", []byte("Amazon product questions and their answers, along with the public product information.")},
	{"amazon-reviews-ml", []byte("The Multilingual Amazon Reviews Corpus")},
	{"answer-reformulation-pds", []byte("Answer Reformulation")},
	{"asr-error-robustness", []byte("Automatic Speech Recognition (ASR) Error Robustness")},
	{"dataworld-linked-acs", []byte("U.S. Census ACS PUMS")},
	{"civic-aws-opendata", []byte("CIViC (Clinical Interpretation of Variants in Cancer)")},
	{"code-mixed-ner", []byte("Multilingual Name Entity Recognition (NER) Datasets with Gazetteer")},
	{"hrsl-cogs", []byte("High Resolution Population Density Maps + Demographic Estimates by CIESIN and Facebook")},
	{"dialoglue", []byte("DialoGLUE: A Natural Language Understanding Benchmark for Task-Oriented Dialogue")},
	{"fashionlocaltriplets", []byte("Fine-grained localized visual similarity and search for fashion.")},
	{"fast-ai-imageclas", []byte("Image classification - fast.ai datasets")},
	{"gdc-fm-ad-phs001179-2-open", []byte("Foundation Medicine Adult Cancer Clinical Dataset (FM-AD)")},
	{"gdc-hcmi-cmdc-phs001486-2-open", []byte("Human Cancer Models Initiative (HCMI) Cancer Model Development Center")},
	{"humor-detection-pds", []byte("Humor Detection from Product Question Answering Systems")},
}

func TestMongoDS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := DefaultConf()
	cfg.DBName = db_test_name
	mds, err := NewMongoDS(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	// fill the test db with test data
	for _, d := range tdata {
		if err := mds.Put(ds.NewKey(d.Key), d.Value); err != nil {
			t.Fatal(err)
		}
	}
	// test Has method
	for _, d := range tdata {
		has, err := mds.Has(ds.NewKey(d.Key))
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatalf("should has key: %s", d.Key)
		}
	}
	// test GetSize method
	for _, d := range tdata {
		size, err := mds.GetSize(ds.NewKey(d.Key))
		if err != nil {
			t.Fatal(err)
		}
		if size != len(d.Value) {
			t.Fatalf("unmatched size, expected: %d, got %d", len(d.Value), size)
		}
	}
	// test Query method
	results, err := mds.Query(query.Query{Prefix: "/"})
	if err != nil {
		t.Fatal(err)
	}
	res, err := results.Rest()
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != len(tdata) {
		t.Fatalf("the number of query results not match, expected: %d, got: %d", len(tdata), len(res))
	}
	for _, item := range res {
		p, ok := getPair(item.Key)
		if !ok {
			t.Fatalf("should has %s in tdata", item.Key)
		}
		if !bytes.Equal(p.Value, item.Value) {
			t.Fatalf("should be equal")
		}
	}
	// test Delete method
	for _, d := range tdata {
		if err := mds.Delete(ds.NewKey(d.Key)); err != nil {
			t.Fatal(err)
		}
	}
	// test Get method when no data in datastore
	for _, d := range tdata {
		_, err := mds.Get(ds.NewKey(d.Key))
		if err != ds.ErrNotFound {
			t.Fatal(err)
		}
	}
	// test Has method when no data in datastore
	for _, d := range tdata {
		has, err := mds.Has(ds.NewKey(d.Key))
		if err != ds.ErrNotFound {
			t.Fatal(err)
		}
		if has {
			t.Fatalf("should not has key: %s", d.Key)
		}
	}
	// test GetSize method when no data in datastore
	for _, d := range tdata {
		size, err := mds.GetSize(ds.NewKey(d.Key))
		if err != ds.ErrNotFound {
			t.Fatal(err)
		}
		if size != -1 {
			t.Fatal("size should be -1")
		}
	}

	// delete would be success even if datastore hasn't save the key/value
	for _, d := range tdata {
		err := mds.Delete(ds.NewKey(d.Key))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func getPair(k string) (Pair, bool) {
	for _, p := range tdata {
		if ds.NewKey(p.Key).String() == k {
			return p, true
		}
	}
	return Pair{}, false
}
