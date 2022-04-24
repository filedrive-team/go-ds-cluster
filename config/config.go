package config

import (
	"encoding/json"
	"io/ioutil"

	"github.com/filedrive-team/go-ds-cluster/shard"
	"go.uber.org/fx"
)

const DefaultConfigPath = ".dscluster"
const DefaultConfigJson = "config.json"
const DefaultMutcaskPath = "mutcask"
const DefaultCaskNum = 8

type Config struct {
	Identity       Identity    `json:"identity"`
	Addresses      Addresses   `json:"addresses"`
	ConfPath       string      `json:"conf_path"`
	Nodes          []Node      `json:"nodes"`
	DisableDelete  bool        `json:"disable_delete"`
	ReadOnlyClient bool        `json:"read_only_client"`
	BootstrapNode  bool        `json:"bootstrap_node"`
	IdentityList   []Identity  `json:"identity_list"`
	Mutcask        MutcaskConf `json:"mutcask"`
}

type MutcaskConf struct {
	Path            string `json:"path"`
	CaskNum         uint32 `json:"cask_num"`
	HintBootReadNum int    `json:"hint_boot_read_num"`
}

type Node struct {
	shard.Node
	Swarm []string `json:"swarm"`
}

type Identity struct {
	PeerID string `json:"peer_id"`
	SK     []byte `json:"sk"`
}

type Addresses struct {
	Swarm []string `json:"swarm"`
}

func LoadConfig(path string) (fx.Option, error) {
	cfg, err := ReadConfig(path + "/config.json")
	if err != nil {
		return nil, err
	}
	cfg.ConfPath = path
	return fx.Provide(func() *Config {
		return cfg
	}), nil
}

func ReadConfig(path string) (*Config, error) {
	cfg := new(Config)
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, cfg)
	if err != nil {
		return nil, err
	}
	return cfg, nil
}
