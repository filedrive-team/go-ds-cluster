package remoteds

import (
	"time"

	cborutil "github.com/filecoin-project/go-cbor-util"
	log "github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p-core/network"
)

var logging = log.Logger("dscluster/p2p/remoteds")

const (
	PROTOCOL_V1 = "/cluster/remoteds/0.0.1"
	PREFIX      = "remoteds"
)

func ReadRequestMsg(s network.Stream, msg *RequestMessage, timeout int) error {
	readDeadline := time.Duration(1e9 * timeout)
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

func WriteRequstMsg(s network.Stream, msg *RequestMessage, timeout int) error {
	writeDeadline := time.Duration(1e9 * timeout)
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

func ReadReplyMsg(s network.Stream, msg *ReplyMessage, timeout int) error {
	readDeadline := time.Duration(1e9 * timeout)
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

func WriteReplyMsg(s network.Stream, msg *ReplyMessage, timeout int) error {
	writeDeadline := time.Duration(1e9 * timeout)
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

func ReadQueryResultEntry(s network.Stream, msg *QueryResultEntry, timeout int) error {
	readDeadline := time.Duration(1e9 * timeout)
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

func WriteQueryResultEntry(s network.Stream, msg *QueryResultEntry, timeout int) error {
	writeDeadline := time.Duration(1e9 * timeout)
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
