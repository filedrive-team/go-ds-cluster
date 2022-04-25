package store

import (
	"context"
	"time"

	"github.com/filedrive-team/go-ds-cluster/core"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"
)

const waitClose = 10

type server struct {
	ctx           context.Context
	host          host.Host
	protocol      protocol.ID
	ds            ds.Datastore
	disableDelete bool
}

func NewStoreServer(ctx context.Context, h host.Host, pid protocol.ID, ds ds.Datastore, disableDelete bool) core.DataNodeServer {
	return &server{
		ctx:           ctx,
		host:          h,
		protocol:      pid,
		ds:            ds,
		disableDelete: disableDelete,
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

func (sv *server) handleStream(s network.Stream) {
	defer func() {
		logging.Info("waitClose start")
		<-time.After(time.Second * waitClose)
		logging.Info("waitClose end")
		s.Close()
	}()
	logging.Info("serve incoming stream")
	reqMsg := new(RequestMessage)

	if err := ReadRequestMsg(s, reqMsg); err != nil {
		logging.Error(err)
		return
	}

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
		sv.query(s, reqMsg)
	default:
		logging.Warnf("unhandled action: %v", reqMsg.Action)
	}
}

func (sv *server) put(s network.Stream, req *RequestMessage) {
	logging.Infof("put %s, value size: %d", req.Key, len(req.Value))
	res := &ReplyMessage{}
	if err := sv.ds.Put(sv.ctx, ds.NewKey(req.Key), req.Value); err != nil {
		res.Code = ErrOthers
		res.Msg = err.Error()
	}
	//res.Msg = "ok"
	if err := WriteReplyMsg(s, res); err != nil {
		logging.Error(err)
	}
}

func (sv *server) has(s network.Stream, req *RequestMessage) {
	res := &ReplyMessage{}
	exists, err := sv.ds.Has(sv.ctx, ds.NewKey(req.Key))
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
	if err := WriteReplyMsg(s, res); err != nil {
		logging.Error(err)
	}
}

func (sv *server) getSize(s network.Stream, req *RequestMessage) {
	res := &ReplyMessage{}
	size, err := sv.ds.GetSize(sv.ctx, ds.NewKey(req.Key))
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
	if err := WriteReplyMsg(s, res); err != nil {
		logging.Error(err)
	}
}

func (sv *server) get(s network.Stream, req *RequestMessage) {
	res := &ReplyMessage{}
	v, err := sv.ds.Get(sv.ctx, ds.NewKey(req.Key))
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
	if err := WriteReplyMsg(s, res); err != nil {
		logging.Error(err)
	}
}

func (sv *server) delete(s network.Stream, req *RequestMessage) {
	res := &ReplyMessage{}

	if sv.disableDelete {
		logging.Infof("delete operation disabled, ignore delete %s", req.Key)
	} else {
		err := sv.ds.Delete(sv.ctx, ds.NewKey(req.Key))
		if err != nil {
			res.Code = ErrOthers
			res.Msg = err.Error()
		}
	}
	if err := WriteReplyMsg(s, res); err != nil {
		logging.Error(err)
	}
}

func (sv *server) query(s network.Stream, req *RequestMessage) {
	qresult, err := sv.ds.Query(sv.ctx, DSQuery(req.Query))
	if err != nil {
		res := &QueryResultEntry{}
		res.Code = ErrOthers
		res.Msg = err.Error()
		if err := WriteQueryResultEntry(s, res); err != nil {
			logging.Error(err)
		}
		return
	}

	for result := range qresult.Next() {
		res := &QueryResultEntry{}
		if result.Error != nil {
			res.Code = ErrOthers
			res.Msg = result.Error.Error()
			if err := WriteQueryResultEntry(s, res); err != nil {
				logging.Error(err)
			}
			return
		}
		res.Key = result.Key
		res.Value = result.Value
		res.Size = int64(result.Size)
		if err := WriteQueryResultEntry(s, res); err != nil {
			logging.Error(err)
			return
		}
	}

}
