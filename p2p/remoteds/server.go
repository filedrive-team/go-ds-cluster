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

const AuthFailed = "unauthorized access denied"
const waitClose = 20

// Interceptor if return true, than send ReplyMessage to sender and skip the next processing
type Interceptor func(req *Request, reply *ReplyMessage) bool

func DefaultUserPrefix(req *Request, reply *ReplyMessage) bool {
	var prefix string
	switch req.Action {
	case ActTouchFile, ActFileInfo, ActDeleteFile, ActListFiles:
		prefix = filepath.Join("/", PREFIX, "default")
		req.InnerFileKey = filepath.Join(prefix, req.Key)
		req.InnerFilePrefix = prefix
	}
	return false
}

type Request struct {
	*RequestMessage
	InnerFileKey    string
	InnerFilePrefix string
}

type server struct {
	ctx           context.Context
	host          host.Host
	protocol      protocol.ID
	ds            ds.Datastore
	fds           ds.Datastore
	disableDelete bool
	timeout       int
	interceptors  []Interceptor
}

func NewStoreServer(ctx context.Context, h host.Host, pid protocol.ID, ds, fds ds.Datastore, disableDelete bool, timeout int, interceptors ...Interceptor) core.DataNodeServer {
	return &server{
		ctx:           ctx,
		host:          h,
		protocol:      pid,
		ds:            ds,
		fds:           fds,
		disableDelete: disableDelete,
		timeout:       timeout,
		interceptors:  interceptors,
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

func (sv *server) AppendInterceptors(interceptors ...Interceptor) {
	sv.interceptors = append(sv.interceptors, interceptors...)
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
	req := &Request{
		RequestMessage: reqMsg,
	}

	reply := &ReplyMessage{}
	for _, handler := range sv.interceptors {
		if handler(req, reply) {
			if err := WriteReplyMsg(s, reply, sv.timeout); err != nil {
				logging.Error(err)
			}
			return
		}
	}

	logging.Infof("req action %v", req.Action)
	switch req.Action {
	case ActGet:
		sv.get(s, req)
	case ActGetSize:
		sv.getSize(s, req)
	case ActHas:
		sv.has(s, req)
	case ActPut:
		sv.put(s, req)
	case ActDelete:
		sv.delete(s, req)
	case ActQuery:
		sv.query(s, req)
	case ActTouchFile:
		sv.touchFile(s, req)
	case ActFileInfo:
		sv.fileInfo(s, req)
	case ActDeleteFile:
		sv.deleteFile(s, req)
	case ActListFiles:
		sv.listFiles(s, req)
	default:
		logging.Warnf("unhandled action: %v", req.Action)
	}
}

func (sv *server) authFailedMag(s network.Stream) {
	res := &ReplyMessage{
		Code: ErrAuthFailed,
		Msg:  AuthFailed,
	}
	if err := WriteReplyMsg(s, res, sv.timeout); err != nil {
		logging.Error(err)
	}
}

func (sv *server) put(s network.Stream, req *Request) {
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

func (sv *server) touchFile(s network.Stream, req *Request) {
	logging.Infof("put %s, value size: %d", req.InnerFileKey, len(req.Value))
	res := &ReplyMessage{}
	if err := sv.fds.Put(ds.NewKey(req.InnerFileKey), req.Value); err != nil {
		res.Code = ErrOthers
		res.Msg = err.Error()
	}
	//res.Msg = "ok"
	if err := WriteReplyMsg(s, res, sv.timeout); err != nil {
		logging.Error(err)
	}
}

func (sv *server) has(s network.Stream, req *Request) {
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

func (sv *server) getSize(s network.Stream, req *Request) {
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

func (sv *server) get(s network.Stream, req *Request) {
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

func (sv *server) fileInfo(s network.Stream, req *Request) {
	res := &ReplyMessage{}
	v, err := sv.fds.Get(ds.NewKey(req.InnerFileKey))
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

func (sv *server) delete(s network.Stream, req *Request) {
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

func (sv *server) deleteFile(s network.Stream, req *Request) {
	res := &ReplyMessage{}

	if sv.disableDelete {
		logging.Infof("delete operation disabled, ignore delete %s", req.Key)
	} else {
		err := sv.fds.Delete(ds.NewKey(req.InnerFileKey))
		if err != nil {
			res.Code = ErrOthers
			res.Msg = err.Error()
		}
	}
	if err := WriteReplyMsg(s, res, sv.timeout); err != nil {
		logging.Error(err)
	}
}

func (sv *server) query(s network.Stream, req *Request) {
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
		res.Key = result.Key
		res.Value = result.Value
		res.Size = int64(result.Size)
		if err := WriteQueryResultEntry(s, res, sv.timeout); err != nil {
			logging.Error(err)
			return
		}
	}
}

func (sv *server) listFiles(s network.Stream, req *Request) {
	qresult, err := sv.fds.Query(dsq.Query{Prefix: req.InnerFileKey})
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
		res.Key = strings.TrimPrefix(result.Key, req.InnerFilePrefix)
		res.Value = result.Value
		res.Size = int64(result.Size)
		if err := WriteQueryResultEntry(s, res, sv.timeout); err != nil {
			logging.Error(err)
			return
		}
	}
}
