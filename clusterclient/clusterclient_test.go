package clusterclient

import (
	"bytes"
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/filedrive-team/go-ds-cluster/config"
	"github.com/filedrive-team/go-ds-cluster/core"
	"github.com/filedrive-team/go-ds-cluster/p2p/store"
	ds "github.com/ipfs/go-datastore"
	log "github.com/ipfs/go-log/v2"
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

var c1cfg = `
{
    "identity": {
        "peer_id":"12D3KooWKarkPBVnpHoapeNmxHc654TBP17zNf8wjp1DvK7jr8mn",
        "sk":"CAESQDq0DDsfyoqSI0ZYAMU2FErig+RdeE7EUjMhRNwFDevdkR8NrRFgtiZK/1TziUEWgXT+pmoEPNvBm5FmNBE+4yk="
    },
    "addresses": {
        "swarm": ["/ip4/0.0.0.0/tcp/9680"]
    },
    "conf_path": "",
    "nodes": [
        {"id":"12D3KooWM1dWYafTFGJc6Kq5XYX6RRbQTCbZ58kXFWsjdHREJtCB","slots":{"start":0,"end":5460},"swarm":["/ip4/0.0.0.0/tcp/9690"]},
        {"id":"12D3KooWQHrRAyak1wYc9u27Tu9HnAqkafdWhZkaohSP834igaiZ","slots":{"start":5461,"end":10922},"swarm":["/ip4/0.0.0.0/tcp/9691"]},
        {"id":"12D3KooWQWLzFTEE9XD2oZph4UifRkE4BWiapsqHjWMnB1R5WRtS","slots":{"start":10923,"end":16383},"swarm":["/ip4/0.0.0.0/tcp/9692"]}
    ]
}
`
var srv1cfg = `
{
    "identity": {
        "peer_id":"12D3KooWM1dWYafTFGJc6Kq5XYX6RRbQTCbZ58kXFWsjdHREJtCB",
        "sk":"CAESQAuZ4TCeDyMU5CePnvpSoXQ4wc+mjfOHvZAOcOdmVLKCplNKqi7nJ5vnn4kfgnkrVdZi0uGHni0H3eItuFXM7iw="
    },
    "addresses": {
        "swarm": ["/ip4/0.0.0.0/tcp/9690"]
    },
    "conf_path": "",
    "nodes": [
        {"id":"12D3KooWM1dWYafTFGJc6Kq5XYX6RRbQTCbZ58kXFWsjdHREJtCB","slots":{"start":0,"end":5460},"swarm":["/ip4/0.0.0.0/tcp/9690"]},
        {"id":"12D3KooWQHrRAyak1wYc9u27Tu9HnAqkafdWhZkaohSP834igaiZ","slots":{"start":5461,"end":10922},"swarm":["/ip4/0.0.0.0/tcp/9691"]},
        {"id":"12D3KooWQWLzFTEE9XD2oZph4UifRkE4BWiapsqHjWMnB1R5WRtS","slots":{"start":10923,"end":16383},"swarm":["/ip4/0.0.0.0/tcp/9692"]}
    ]
}
`
var srv2cfg = `
{
    "identity": {
        "peer_id":"12D3KooWQHrRAyak1wYc9u27Tu9HnAqkafdWhZkaohSP834igaiZ",
        "sk":"CAESQCI0tvxduyFB+S7+YACHiVb91ywGRsahgFCE/s8z2OyU1w49YyaAS2hl/eS/+POy8TIgbYMj+fye7d0ePPHQpZY="
    },
    "addresses": {
        "swarm": ["/ip4/0.0.0.0/tcp/9691"]
    },
    "conf_path": "",
    "nodes": [
        {"id":"12D3KooWM1dWYafTFGJc6Kq5XYX6RRbQTCbZ58kXFWsjdHREJtCB","slots":{"start":0,"end":5460},"swarm":["/ip4/0.0.0.0/tcp/9690"]},
        {"id":"12D3KooWQHrRAyak1wYc9u27Tu9HnAqkafdWhZkaohSP834igaiZ","slots":{"start":5461,"end":10922},"swarm":["/ip4/0.0.0.0/tcp/9691"]},
        {"id":"12D3KooWQWLzFTEE9XD2oZph4UifRkE4BWiapsqHjWMnB1R5WRtS","slots":{"start":10923,"end":16383},"swarm":["/ip4/0.0.0.0/tcp/9692"]}
    ]
}
`
var srv3cfg = `
{
    "identity": {
        "peer_id":"12D3KooWQWLzFTEE9XD2oZph4UifRkE4BWiapsqHjWMnB1R5WRtS",
        "sk":"CAESQKQkW+gjZBPJKTIpJzAw3GSakg9px2VmkoH8rQu2ZSTm2kGDmCDwAR3ZaXb18+auXCfTwrg+kYAf+Ie6y+fL+s8="
    },
    "addresses": {
        "swarm": ["/ip4/0.0.0.0/tcp/9692"]
    },
    "conf_path": "",
    "nodes": [
        {"id":"12D3KooWM1dWYafTFGJc6Kq5XYX6RRbQTCbZ58kXFWsjdHREJtCB","slots":{"start":0,"end":5460},"swarm":["/ip4/0.0.0.0/tcp/9690"]},
        {"id":"12D3KooWQHrRAyak1wYc9u27Tu9HnAqkafdWhZkaohSP834igaiZ","slots":{"start":5461,"end":10922},"swarm":["/ip4/0.0.0.0/tcp/9691"]},
        {"id":"12D3KooWQWLzFTEE9XD2oZph4UifRkE4BWiapsqHjWMnB1R5WRtS","slots":{"start":10923,"end":16383},"swarm":["/ip4/0.0.0.0/tcp/9692"]}
    ]
}
`

func TestClusterClient(t *testing.T) {
	log.SetLogLevel("*", "info")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var err error

	srv1Cfg, err := cfgFromString(srv1cfg)
	if err != nil {
		t.Fatal(err)
	}

	srv1, err := serverFromCfg(ctx, srv1Cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srv1.Close()
	srv1.Serve()

	srv2Cfg, err := cfgFromString(srv2cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv2, err := serverFromCfg(ctx, srv2Cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srv2.Close()
	srv2.Serve()

	srv3Cfg, err := cfgFromString(srv3cfg)
	if err != nil {
		t.Fatal(err)
	}
	srv3, err := serverFromCfg(ctx, srv3Cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer srv3.Close()
	srv3.Serve()

	clientCfg, err := cfgFromString(c1cfg)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewClusterClient(ctx, clientCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for i, item := range tdata {
		err = client.Put(ds.NewKey(item.Key), item.Value)
		if err != nil {
			t.Fatalf("index %d, key: %s err: %s", i, item.Key, err)
		}
	}

	for _, item := range tdata {
		has, err := client.Has(ds.NewKey(item.Key))
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatalf("should has %s", item.Key)
		}
	}

	for _, item := range tdata {
		size, err := client.GetSize(ds.NewKey(item.Key))
		if err != nil {
			t.Fatal(err)
		}
		if size != len(item.Value) {
			t.Fatalf("%s size not match", item.Key)
		}
	}

	for _, item := range tdata {
		v, err := client.Get(ds.NewKey(item.Key))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v, item.Value) {
			t.Fatal("retrived value not match")
		}
	}

	for _, item := range tdata {
		err := client.Delete(ds.NewKey(item.Key))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestClusterClientByConfGen(t *testing.T) {
	log.SetLogLevel("*", "info")
	cluster_node_num := 3
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var err error

	srvCfgs, err := config.GenClusterConf(cluster_node_num)
	if err != nil {
		t.Fatal(err)
	}
	var sw sync.WaitGroup
	sw.Add(cluster_node_num)
	for _, cfg := range srvCfgs {
		go func(ctx context.Context, cfg *config.Config) {
			srv, err := serverFromCfg(ctx, cfg)
			if err != nil {
				sw.Done()
				t.Error(err)
				return
			}
			sw.Done()
			srv.Serve()

			<-ctx.Done()
			srv.Close()
		}(ctx, cfg)
	}
	sw.Wait()

	clientCfg, err := config.GenClientConf()
	if err != nil {
		t.Fatal(err)
	}
	clientCfg.Nodes = srvCfgs[1].Nodes
	client, err := NewClusterClient(ctx, clientCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for i, item := range tdata {
		err = client.Put(ds.NewKey(item.Key), item.Value)
		if err != nil {
			t.Fatalf("index %d, key: %s err: %s", i, item.Key, err)
		}
	}

	for _, item := range tdata {
		has, err := client.Has(ds.NewKey(item.Key))
		if err != nil {
			t.Fatal(err)
		}
		if !has {
			t.Fatalf("should has %s", item.Key)
		}
	}

	for _, item := range tdata {
		size, err := client.GetSize(ds.NewKey(item.Key))
		if err != nil {
			t.Fatal(err)
		}
		if size != len(item.Value) {
			t.Fatalf("%s size not match", item.Key)
		}
	}

	for _, item := range tdata {
		v, err := client.Get(ds.NewKey(item.Key))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(v, item.Value) {
			t.Fatal("retrived value not match")
		}
	}

	for _, item := range tdata {
		err := client.Delete(ds.NewKey(item.Key))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func cfgFromString(str string) (*config.Config, error) {
	cfg := &config.Config{}
	err := json.Unmarshal([]byte(str), cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}

func serverFromCfg(ctx context.Context, cfg *config.Config) (core.DataNodeServer, error) {
	h, err := store.HostFromConf(cfg)
	if err != nil {
		return nil, err
	}

	memStore := ds.NewMapDatastore()
	return store.NewStoreServer(ctx, h, store.PROTOCOL_V1, memStore), nil
}
