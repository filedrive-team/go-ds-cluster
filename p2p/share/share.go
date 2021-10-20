package share

import (
	"time"

	log "github.com/ipfs/go-log/v2"
)

var logging = log.Logger("dscluster/p2p/share")

const (
	PROTOCOL_V1 = "/cluster/share/0.0.1"
)

var readDeadline = time.Second * 20
var writeDeadline = time.Second * 20
