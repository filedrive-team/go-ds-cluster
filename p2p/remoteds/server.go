package remoteds

import (
	"context"
	"path/filepath"
	"strings"
	"time"

	"github.com/filedrive-team/go-ds-cluster/core"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"
)

const authFailed = "unauthorized access denied"
const waitClose = 20

type server struct {
	ctx           context.Context
	host          host.Host
	protocol      protocol.ID
	ds            ds.Datastore
	fds           ds.Datastore
	disableDelete bool
	timeout       int
	isValid       func(token string) bool
	userPrefix    func(token string) string
}

func NewStoreServer(ctx context.Context, h host.Host, pid protocol.ID, ds, fds ds.Datastore, disableDelete bool, timeout int, verify func(token string) bool, userPrefix func(token string) string) core.DataNodeServer {
	return &server{
		ctx:           ctx,
		host:          h,
		protocol:      pid,
		ds:            ds,
		fds:           fds,
		disableDelete: disableDelete,
		timeout:       timeout,
		isValid:       verify,
		userPrefix:    userPrefix,
	}
}

func (sv *server) Close() error {
	var err error
	if err = sv.ds.Close(); err != nil {
		logging.Error(err)
	}
	if err = sv.host.Close(); err != nil {
		logging.Error(err)
	}
	return err
}

func (sv *server) Serve() {
	logging.Info("data node server set stream handler")
	sv.host.SetStreamHandler(sv.protocol, sv.handleStream)
}

func (sv *server) verify(msg *RequestMessage) bool {
	return sv.isValid(msg.AccessToken)
}

func (sv *server) handleStream(s network.Stream) {
	defer func() {
		logging.Info("waitClose start")
		<-time.After(time.Second * waitClose)
		logging.Info("waitClose end")
		s.Close()
	}()
	logging.Info("serve incoming stream")
	reqMsg := new(RequestMessage)

	if err := ReadRequestMsg(s, reqMsg, sv.timeout); err != nil {
		logging.Error(err)
		return
	}
	if !sv.verify(reqMsg) {
		logging.Warn(authFailed, *reqMsg)
		sv.authFailedMag(s)
		return
	}
	prefix := filepath.Join("/", PREFIX, sv.userPrefix(reqMsg.AccessToken))
	reqMsg.Key = filepath.Join(prefix, reqMsg.Key)

	logging.Infof("req action %v", reqMsg.Action)
	switch reqMsg.Action {
	case ActGet:
		sv.get(s, reqMsg)
	case ActGetSize:
		sv.getSize(s, reqMsg)
	case ActHas:
		sv.has(s, reqMsg)
	case ActPut:
		sv.put(s, reqMsg)
	case ActDelete:
		sv.delete(s, reqMsg)
	case ActQuery:
		sv.query(s, reqMsg, prefix)
	case ActTouchFile:
		sv.touchFile(s, reqMsg)
	case ActFileInfo:
		sv.fileInfo(s, reqMsg)
	case ActDeleteFile:
		sv.deleteFile(s, reqMsg)
	case ActListFiles:
		sv.listFiles(s, reqMsg, prefix)
	default:
		logging.Warnf("unhandled action: %v", reqMsg.Action)
	}
}

func (sv *server) authFailedMag(s network.Stream) {
	res := &ReplyMessage{
		Code: ErrAuthFailed,
		Msg:  authFailed,
	}
	if err := WriteReplyMsg(s, res, sv.timeout); err != nil {
		logging.Error(err)
	}
}

func (sv *server) put(s network.Stream, req *RequestMessage) {
	logging.Infof("put %s, value size: %d", req.Key, len(req.Value))
	res := &ReplyMessage{}
	if err := sv.ds.Put(ds.NewKey(req.Key), req.Value); err != nil {
		res.Code = ErrOthers
		res.Msg = err.Error()
	}
	//res.Msg = "ok"
	if err := WriteReplyMsg(s, res, sv.timeout); err != nil {
		logging.Error(err)
	}
}

func (sv *server) touchFile(s network.Stream, req *RequestMessage) {
	logging.Infof("put %s, value size: %d", req.Key, len(req.Value))
	res := &ReplyMessage{}
	if err := sv.fds.Put(ds.NewKey(req.Key), req.Value); err != nil {
		res.Code = ErrOthers
		res.Msg = err.Error()
	}
	//res.Msg = "ok"
	if err := WriteReplyMsg(s, res, sv.timeout); err != nil {
		logging.Error(err)
	}
}

func (sv *server) has(s network.Stream, req *RequestMessage) {
	res := &ReplyMessage{}
	exists, err := sv.ds.Has(ds.NewKey(req.Key))
	if err != nil {
		if err == ds.ErrNotFound {
			res.Code = ErrNotFound
		} else {
			res.Code = ErrOthers
		}
		res.Msg = err.Error()
	} else {
		res.Exists = exists
	}
	if err := WriteReplyMsg(s, res, sv.timeout); err != nil {
		logging.Error(err)
	}
}

func (sv *server) getSize(s network.Stream, req *RequestMessage) {
	res := &ReplyMessage{}
	size, err := sv.ds.GetSize(ds.NewKey(req.Key))
	if err != nil {
		if err == ds.ErrNotFound {
			res.Code = ErrNotFound
		} else {
			res.Code = ErrOthers
		}
		res.Msg = err.Error()
	} else {
		res.Size = int64(size)
	}
	if err := WriteReplyMsg(s, res, sv.timeout); err != nil {
		logging.Error(err)
	}
}

func (sv *server) get(s network.Stream, req *RequestMessage) {
	res := &ReplyMessage{}
	v, err := sv.ds.Get(ds.NewKey(req.Key))
	if err != nil {
		if err == ds.ErrNotFound {
			res.Code = ErrNotFound
		} else {
			res.Code = ErrOthers
		}
		res.Msg = err.Error()
	} else {
		res.Value = v
	}
	if err := WriteReplyMsg(s, res, sv.timeout); err != nil {
		logging.Error(err)
	}
}

func (sv *server) fileInfo(s network.Stream, req *RequestMessage) {
	res := &ReplyMessage{}
	v, err := sv.fds.Get(ds.NewKey(req.Key))
	if err != nil {
		if err == ds.ErrNotFound {
			res.Code = ErrNotFound
		} else {
			res.Code = ErrOthers
		}
		res.Msg = err.Error()
	} else {
		res.Value = v
	}
	if err := WriteReplyMsg(s, res, sv.timeout); err != nil {
		logging.Error(err)
	}
}

func (sv *server) delete(s network.Stream, req *RequestMessage) {
	res := &ReplyMessage{}

	if sv.disableDelete {
		logging.Infof("delete operation disabled, ignore delete %s", req.Key)
	} else {
		err := sv.ds.Delete(ds.NewKey(req.Key))
		if err != nil {
			res.Code = ErrOthers
			res.Msg = err.Error()
		}
	}
	if err := WriteReplyMsg(s, res, sv.timeout); err != nil {
		logging.Error(err)
	}
}

func (sv *server) deleteFile(s network.Stream, req *RequestMessage) {
	res := &ReplyMessage{}

	if sv.disableDelete {
		logging.Infof("delete operation disabled, ignore delete %s", req.Key)
	} else {
		err := sv.fds.Delete(ds.NewKey(req.Key))
		if err != nil {
			res.Code = ErrOthers
			res.Msg = err.Error()
		}
	}
	if err := WriteReplyMsg(s, res, sv.timeout); err != nil {
		logging.Error(err)
	}
}

func (sv *server) query(s network.Stream, req *RequestMessage, prefix string) {
	qresult, err := sv.ds.Query(DSQuery(req.Query))
	if err != nil {
		res := &QueryResultEntry{}
		res.Code = ErrOthers
		res.Msg = err.Error()
		if err := WriteQueryResultEntry(s, res, sv.timeout); err != nil {
			logging.Error(err)
		}
		return
	}

	for result := range qresult.Next() {
		res := &QueryResultEntry{}
		if result.Error != nil {
			res.Code = ErrOthers
			res.Msg = result.Error.Error()
			if err := WriteQueryResultEntry(s, res, sv.timeout); err != nil {
				logging.Error(err)
			}
			return
		}
		res.Key = strings.TrimPrefix(result.Key, prefix)
		res.Value = result.Value
		res.Size = int64(result.Size)
		if err := WriteQueryResultEntry(s, res, sv.timeout); err != nil {
			logging.Error(err)
			return
		}
	}
}

func (sv *server) listFiles(s network.Stream, req *RequestMessage, prefix string) {
	qresult, err := sv.fds.Query(dsq.Query{Prefix: req.Key})
	if err != nil {
		res := &QueryResultEntry{}
		res.Code = ErrOthers
		res.Msg = err.Error()
		if err := WriteQueryResultEntry(s, res, sv.timeout); err != nil {
			logging.Error(err)
		}
		return
	}

	for result := range qresult.Next() {
		res := &QueryResultEntry{}
		if result.Error != nil {
			res.Code = ErrOthers
			res.Msg = result.Error.Error()
			if err := WriteQueryResultEntry(s, res, sv.timeout); err != nil {
				logging.Error(err)
			}
			return
		}
		res.Key = strings.TrimPrefix(result.Key, prefix)
		res.Value = result.Value
		res.Size = int64(result.Size)
		if err := WriteQueryResultEntry(s, res, sv.timeout); err != nil {
			logging.Error(err)
			return
		}
	}
}
