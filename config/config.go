package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	mrand "math/rand"

	"github.com/filedrive-team/go-ds-cluster/shard"
	"go.uber.org/fx"
)

type Config struct {
	Identity  Identity  `json:"identity"`
	Addresses Addresses `json:"addresses"`
	ConfPath  string    `json:"conf_path"`
	Nodes     []Node    `json:"nodes"`
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

func randPortNumber() string {
	mrand.Seed(time.Now().Unix() * int64(mrand.Intn(9999)))
	r := mrand.Float64()
	m := 1000 + 9000*r
	return fmt.Sprintf("%.0f", m)
}
