package config

import (
	"encoding/json"
	"io/ioutil"

	"github.com/filedrive-team/go-ds-cluster/shard"
	"go.uber.org/fx"
)

type Config struct {
	Identity  Identity     `json:"identity"`
	Addresses Addresses    `json:"addresses"`
	ConfPath  string       `json:"conf_path"`
	Nodes     []shard.Node `json:"nodes"`
}

type Identity struct {
	PeerID string `json:"peer_id"`
	SK     []byte `json:"sk"`
}

type Addresses struct {
	Swarm []string `json:"swarm"`
}

func LoadConfig(path string) (fx.Option, error) {
	cfg, err := loadConfig(path + "/config.json")
	if err != nil {
		return nil, err
	}
	cfg.ConfPath = path
	return fx.Provide(func() *Config {
		return cfg
	}), nil
}

func loadConfig(path string) (*Config, error) {
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
