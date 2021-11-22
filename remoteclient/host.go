package remoteclient

import (
	"github.com/filedrive-team/go-ds-cluster/p2p"
	"github.com/filedrive-team/go-ds-cluster/utils"
	"github.com/libp2p/go-libp2p-core/host"
)

func HostForRemoteClient(cfg *Config) (host.Host, error) {
	return p2p.MakeHost(utils.RandPort(), cfg.SwarmKey)
}
