package ipfsplugin

import (
	"context"
	"fmt"

	"github.com/ipfs/go-ipfs/plugin"
	"github.com/ipfs/go-ipfs/repo"
	"github.com/ipfs/go-ipfs/repo/fsrepo"

	"github.com/filedrive-team/go-ds-cluster/clusterclient"
	clustercfg "github.com/filedrive-team/go-ds-cluster/config"
)

// Plugins is exported list of plugins that will be loaded
var Plugins = []plugin.Plugin{
	&mongodsPlugin{},
}

type mongodsPlugin struct{}

var _ plugin.PluginDatastore = (*mongodsPlugin)(nil)

func (*mongodsPlugin) Name() string {
	return "ds-cluster"
}

func (*mongodsPlugin) Version() string {
	return "0.0.1"
}

func (*mongodsPlugin) Init(_ *plugin.Environment) error {
	return nil
}

func (*mongodsPlugin) DatastoreTypeName() string {
	return "clusterds"
}

type datastoreConfig struct {
	cfg string
}

func (*mongodsPlugin) DatastoreConfigParser() fsrepo.ConfigFromMap {
	return func(params map[string]interface{}) (fsrepo.DatastoreConfig, error) {
		var c datastoreConfig
		var ok bool

		c.cfg, ok = params["cfg"].(string)
		if !ok {
			return nil, fmt.Errorf("'cfg' field is missing or not string")
		}
		return &c, nil
	}
}

func (c *datastoreConfig) DiskSpec() fsrepo.DiskSpec {
	return map[string]interface{}{
		"type": "mongods",
		"cfg":  c.cfg,
	}
}

func (c *datastoreConfig) Create(path string) (repo.Datastore, error) {
	cfg, err := clustercfg.ReadConfig(c.cfg)
	if err != nil {
		return nil, err
	}
	cc, err := clusterclient.NewClusterClient(context.Background(), cfg)
	if err != nil {
		return nil, err
	}
	return cc, nil
}
