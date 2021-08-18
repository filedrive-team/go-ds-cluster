package store

import (
	"context"
	"io/ioutil"
	"time"

	"github.com/filedrive-team/go-ds-cluster/core"
	storepb "github.com/filedrive-team/go-ds-cluster/p2p/store/pb"
	ggio "github.com/gogo/protobuf/io"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"
	"google.golang.org/protobuf/proto"
)

type server struct {
	ctx      context.Context
	host     host.Host
	protocol protocol.ID
	ds       ds.Datastore
}

func NewStoreServer(ctx context.Context, h host.Host, pid protocol.ID, ds ds.Datastore) core.DataNodeServer {
	return &server{
		ctx:      ctx,
		host:     h,
		protocol: pid,
		ds:       ds,
	}
}

func (sv *server) Close() error {
	return sv.host.Close()
}

func (sv *server) Serve() {
	logging.Info("data node server set stream handler")
	sv.host.SetStreamHandler(sv.protocol, sv.handleStream)
}

func (sv *server) handleStream(s network.Stream) {
	defer s.Close()
	logging.Info("server incoming stream")
	reqMsg := &storepb.StoreRequest{}
	buf, err := ioutil.ReadAll(s)
	if err != nil {
		logging.Infof("server read buf error %v", err)

		return
	}

	err = proto.Unmarshal(buf, reqMsg)
	if err != nil {
		logging.Infof("server proto unmarshal error %v", reqMsg)

		return
	}

	ss, err := sv.host.NewStream(sv.ctx, s.Conn().RemotePeer(), sv.protocolReply)
	if err != nil {
		logging.Info(err)
		return
	}

	logging.Infof("req action %v", reqMsg.GetAction())
	switch reqMsg.GetAction() {
	case storepb.Action_Get:
		sv.get(ss, reqMsg)
	case storepb.Action_GetSize:
		sv.getSize(s, reqMsg.GetKey())
	case storepb.Action_Has:
		sv.has(s, reqMsg.GetKey())
	case storepb.Action_Put:
		sv.put(s, reqMsg.GetKey(), reqMsg.GetValue())
	case storepb.Action_Delete:
		sv.delete(s, reqMsg.GetKey())
	default:
		logging.Warnf("unhandled action: %v", reqMsg.GetAction())
	}
}

func (sv *server) put(s network.Stream, key string, value []byte) {
	logging.Infof("put %s, value size: %d", key, len(value))
	res := &storepb.StoreResponse{}
	err := sv.ds.Put(ds.NewKey(key), value)
	if err != nil {
		res.Code = storepb.ErrCode_Others
		res.Msg = err.Error()
	}
	res.Msg = "ok"
	sv.sendMsg(s, res)
}

func (sv *server) has(s network.Stream, key string) {
	res := &storepb.StoreResponse{}
	exists, err := sv.ds.Has(ds.NewKey(key))
	if err != nil {
		if err == ds.ErrNotFound {
			res.Code = storepb.ErrCode_None
		} else {
			res.Code = storepb.ErrCode_Others
		}
		res.Msg = err.Error()
	} else {
		res.Has = exists
	}
	sv.sendMsg(s, res)
}

func (sv *server) getSize(s network.Stream, key string) {
	res := &storepb.StoreResponse{}
	size, err := sv.ds.GetSize(ds.NewKey(key))
	if err != nil {
		if err == ds.ErrNotFound {
			res.Code = storepb.ErrCode_NotFound
		} else {
			res.Code = storepb.ErrCode_Others
		}
		res.Msg = err.Error()
	} else {
		res.Size = int64(size)
	}
	sv.sendMsg(s, res)
}

func (sv *server) get(s network.Stream, reqMsg *storepb.StoreRequest) {
	res := &storepb.StoreResponse{}
	v, err := sv.ds.Get(ds.NewKey(reqMsg.GetKey()))
	if err != nil {
		if err == ds.ErrNotFound {
			res.Code = storepb.ErrCode_NotFound
		} else {
			res.Code = storepb.ErrCode_Others
		}
		res.Msg = err.Error()
	} else {
		res.Value = v
	}
	res.Id = reqMsg.Id
	sv.sendMsg(s, res)
}

func (sv *server) delete(s network.Stream, key string) {
	res := &storepb.StoreResponse{}
	err := sv.ds.Delete(ds.NewKey(key))
	if err != nil {
		res.Code = storepb.ErrCode_Others
		res.Msg = err.Error()
	}
	sv.sendMsg(s, res)
}

func (sv *server) sendMsg(s network.Stream, res *storepb.StoreResponse) {
	_ = s.SetWriteDeadline(time.Now().Add(time.Second))
	logging.Infof("server reply msg: %v", res)
	b, err := proto.Marshal(res)
	if err != nil {
		logging.Warn(err)
		return
	}
	logging.Infof("msg bytes: %v", b)
	writer := ggio.NewFullWriter(s)
	err = writer.WriteMsg(res)
	if err != nil {
		logging.Error(err)
		return
	}
	defer writer.Close()
}
