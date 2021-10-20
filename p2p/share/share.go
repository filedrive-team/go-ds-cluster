// share cluster nodes info
package share

import (
	"time"

	cborutil "github.com/filecoin-project/go-cbor-util"
	log "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/network"
)

var logging = log.Logger("dscluster/p2p/share")

const (
	PROTOCOL_V1 = "/cluster/share/0.0.1"
)

var readDeadline = time.Second * 20
var writeDeadline = time.Second * 20

type InfoType uint8

const (
	InfoClusterNodes InfoType = 1 + iota
)

type ErrCode uint8

const (
	ErrNone ErrCode = iota
	ErrNotFound

	ErrOthers = 100
)

type ShareRequest struct {
	Type InfoType
}

type ShareReply struct {
	Code ErrCode
	Msg  string
	Type InfoType
	Info []byte
}

func ReadRequest(s network.Stream, msg *ShareRequest) error {
	if err := s.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
		return err
	}
	if err := cborutil.ReadCborRPC(s, msg); err != nil {
		_ = s.SetReadDeadline(time.Time{})
		return err
	}
	_ = s.SetReadDeadline(time.Time{})
	return nil
}

func WriteRequst(s network.Stream, msg *ShareRequest) error {
	if err := s.SetWriteDeadline(time.Now().Add(writeDeadline)); err != nil {
		return err
	}
	if err := cborutil.WriteCborRPC(s, msg); err != nil {
		_ = s.SetWriteDeadline(time.Time{})
		return err
	}
	_ = s.SetWriteDeadline(time.Time{})
	return nil
}

func ReadReply(s network.Stream, msg *ShareReply) error {
	if err := s.SetReadDeadline(time.Now().Add(readDeadline)); err != nil {
		return err
	}
	if err := cborutil.ReadCborRPC(s, msg); err != nil {
		_ = s.SetReadDeadline(time.Time{})
		return err
	}
	_ = s.SetReadDeadline(time.Time{})
	return nil
}

func WriteReply(s network.Stream, msg *ShareReply) error {
	if err := s.SetWriteDeadline(time.Now().Add(writeDeadline)); err != nil {
		return err
	}
	if err := cborutil.WriteCborRPC(s, msg); err != nil {
		_ = s.SetWriteDeadline(time.Time{})
		return err
	}
	_ = s.SetWriteDeadline(time.Time{})
	return nil
}
