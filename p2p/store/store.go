package store

import (
	"context"
	"io/ioutil"

	storepb "github.com/filedrive-team/go-ds-cluster/p2p/store/pb"
	ggio "github.com/gogo/protobuf/io"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/protocol"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
)

const (
	PROTOCOL_V1 = "/cluster/store/0.0.1"
)

type client struct {
	ctx       context.Context
	src       host.Host
	target    peer.AddrInfo
	connected bool
	protocol  protocol.ID
}

func (cl *client) IsTargetConnected() bool {
	return cl.connected
}

func (cl *client) ConnectTarget() error {
	if cl.IsTargetConnected() {
		return nil
	}
	err := cl.src.Connect(cl.ctx, cl.target)
	if err != nil {
		return err
	}
	cl.connected = true
	return nil
}

func (cl *client) SendMessage(ctx context.Context, req *storepb.StoreRequest, res chan *storepb.StoreResponse) {
	go func() {
		resMsg := new(storepb.StoreResponse)
		s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
		if err != nil {
			resMsg.Code = storepb.ErrCode_Others
			resMsg.Msg = err.Error()
			res <- resMsg
			return
		}
		writer := ggio.NewFullWriter(s)
		err = writer.WriteMsg(req)
		if err != nil {
			s.Reset()
			resMsg.Code = storepb.ErrCode_Others
			resMsg.Msg = err.Error()
			res <- resMsg
			return
		}
		defer writer.Close()

		buf, err := ioutil.ReadAll(s)
		if err != nil {
			s.Reset()
			resMsg.Code = storepb.ErrCode_Others
			resMsg.Msg = err.Error()
			res <- resMsg
			return
		}

		err = proto.Unmarshal(buf, resMsg)
		if err != nil {
			s.Reset()
			resMsg.Code = storepb.ErrCode_Others
			resMsg.Msg = err.Error()
			res <- resMsg
			return
		}
		res <- resMsg
	}()
}

func (cl *client) Put(key string, value []byte) error {
	err := cl.ConnectTarget()
	if err != nil {
		return err
	}

	req := &storepb.StoreRequest{
		Key:    key,
		Value:  value,
		Action: storepb.Action_Put,
	}

	res := make(chan *storepb.StoreResponse)
	cl.SendMessage(cl.ctx, req, res)

	resMsg := <-res
	if resMsg.GetCode() != storepb.ErrCode_None {
		return xerrors.New(resMsg.GetMsg())
	}
	return nil
}

func (cl *client) Delete(key string) error {
	err := cl.ConnectTarget()
	if err != nil {
		return err
	}

	req := &storepb.StoreRequest{
		Key:    key,
		Action: storepb.Action_Delete,
	}

	res := make(chan *storepb.StoreResponse)
	cl.SendMessage(cl.ctx, req, res)

	resMsg := <-res
	if resMsg.GetCode() != storepb.ErrCode_None {
		if resMsg.GetCode() == storepb.ErrCode_NotFound {
			return nil
		}
		return xerrors.New(resMsg.GetMsg())
	}
	return nil
}

func (cl *client) Get(key string) (value []byte, err error) {
	err = cl.ConnectTarget()
	if err != nil {
		return nil, err
	}

	req := &storepb.StoreRequest{
		Key:    key,
		Action: storepb.Action_Get,
	}

	res := make(chan *storepb.StoreResponse)
	cl.SendMessage(cl.ctx, req, res)

	resMsg := <-res
	if resMsg.GetCode() != storepb.ErrCode_None {
		if resMsg.GetCode() == storepb.ErrCode_NotFound {
			return nil, ds.ErrNotFound
		}
		return nil, xerrors.New(resMsg.GetMsg())
	}
	return resMsg.GetValue(), nil
}

func (cl *client) Has(key string) (exists bool, err error) {
	err = cl.ConnectTarget()
	if err != nil {
		return false, err
	}

	req := &storepb.StoreRequest{
		Key:    key,
		Action: storepb.Action_Has,
	}

	res := make(chan *storepb.StoreResponse)
	cl.SendMessage(cl.ctx, req, res)

	resMsg := <-res
	if resMsg.GetCode() != storepb.ErrCode_None {
		if resMsg.GetCode() == storepb.ErrCode_NotFound {
			return false, nil
		}
		return false, xerrors.New(resMsg.GetMsg())
	}
	return resMsg.GetHas(), nil
}

func (cl *client) GetSize(key string) (size int, err error) {
	err = cl.ConnectTarget()
	if err != nil {
		return -1, err
	}

	req := &storepb.StoreRequest{
		Key:    key,
		Action: storepb.Action_GetSize,
	}

	res := make(chan *storepb.StoreResponse)
	cl.SendMessage(cl.ctx, req, res)

	resMsg := <-res
	if resMsg.GetCode() != storepb.ErrCode_None {
		if resMsg.GetCode() == storepb.ErrCode_NotFound {
			return -1, ds.ErrNotFound
		}
		return -1, xerrors.New(resMsg.GetMsg())
	}
	return int(resMsg.GetSize()), nil
}
