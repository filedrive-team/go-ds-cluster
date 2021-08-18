package store

import (
	"time"

	cborutil "github.com/filecoin-project/go-cbor-util"
	log "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/network"
)

var logging = log.Logger("dscluster/p2p/store")

const (
	PROTOCOL_V1         = "/cluster/store/0.0.1"
	PROTOCOL_REQUEST_V1 = "/cluster/store/request/0.0.1"
	PROTOCOL_REPLY_V1   = "/cluster/store/reply/0.0.1"
)

var readDeadline = time.Second * 2
var writeDeadline = time.Second * 5

func ReadRequestMsg(s network.Stream, msg *RequestMessage) error {
	if err := s.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
		return err
	}
	if err := cborutil.ReadCborRPC(s, msg); err != nil {
		return err
	}
	return nil
}

func WriteReplyMsg(s network.Stream, msg *ReplyMessage) error {
	if err := s.SetWriteDeadline(time.Now().Add(writeDeadline)); err != nil {
		return err
	}
	if err := cborutil.WriteCborRPC(s, msg); err != nil {
		return err
	}
	return nil
}