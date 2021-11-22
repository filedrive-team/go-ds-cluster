package remoteclient

import (
	"encoding/json"
	"io/ioutil"
)

type Config struct {
	AccessToken string `json:"access_token"`
	Bucket      string `json:"bucket"`
	Timeout     int    `json:"timeout"`
	SwarmKey    string `json:"swarmkey"`
	Target      string `json:"target"`
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
