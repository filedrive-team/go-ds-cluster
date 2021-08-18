package store

import (
	"context"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/filedrive-team/go-ds-cluster/core"
	storepb "github.com/filedrive-team/go-ds-cluster/p2p/store/pb"
	ggio "github.com/gogo/protobuf/io"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"golang.org/x/xerrors"
	"google.golang.org/protobuf/proto"
)

type client struct {
	ctx           context.Context
	src           host.Host
	target        peer.AddrInfo
	connected     bool
	protocol      protocol.ID
	protocolReply protocol.ID
	counter       int
	taskMap       map[string]chan *storepb.StoreResponse
}

func NewStoreClient(ctx context.Context, src host.Host, target peer.AddrInfo, pid, replyPid protocol.ID) core.DataNodeClient {
	return &client{
		ctx:           ctx,
		src:           src,
		target:        target,
		protocol:      pid,
		protocolReply: replyPid,
		taskMap:       make(map[string]chan *storepb.StoreResponse),
	}
}

func (cl *client) nextId() string {
	cl.counter++
	return fmt.Sprintf("id-%d", cl.counter)
}

func (cl *client) Close() error {
	return cl.src.Close()
}

func (cl *client) IsTargetConnected() bool {
	return cl.connected
}

func (cl *client) ConnectTarget() error {
	if cl.IsTargetConnected() {
		return nil
	}
	cl.src.Peerstore().AddAddrs(cl.target.ID, cl.target.Addrs, peerstore.PermanentAddrTTL)
	cl.connected = true
	return nil
}

func (cl *client) SetHandle() {
	go func() {
		cl.src.SetStreamHandler(cl.protocolReply, func(s network.Stream) {
			defer s.Close()
			logging.Info("client incoming stream")
			resMsg := &storepb.StoreResponse{}
			buf, err := ioutil.ReadAll(s)
			if err != nil {
				logging.Infof("server read buf error %v", err)
				return
			}

			err = proto.Unmarshal(buf, resMsg)
			if err != nil {
				logging.Infof("server proto unmarshal error %v", resMsg)
				return
			}

			if ch, ok := cl.taskMap[resMsg.Id]; ok {
				delete(cl.taskMap, resMsg.Id)
				ch <- resMsg
			} else {
				logging.Warn(resMsg)
			}
		})
	}()
}

func (cl *client) SendMessage(ctx context.Context, req *storepb.StoreRequest, res chan *storepb.StoreResponse) {
	//go func() {
	resMsg := new(storepb.StoreResponse)
	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		resMsg.Code = storepb.ErrCode_Others
		resMsg.Msg = err.Error()
		res <- resMsg
		return
	}
	logging.Info("set up stream to target")
	writer := ggio.NewFullWriter(s)
	err = writer.WriteMsg(req)
	if err != nil {
		s.Reset()
		resMsg.Code = storepb.ErrCode_Others
		resMsg.Msg = err.Error()
		res <- resMsg
		return
	}
	defer s.Close()
	//defer writer.Close()
	logging.Info("finish write to stream")
	go func() {

		logging.Info("start to read reply")
		buf, err := ioutil.ReadAll(s)
		if err != nil {
			logging.Infof("client read response error %v", err)
			resMsg.Code = storepb.ErrCode_Others
			resMsg.Msg = err.Error()
			res <- resMsg
			return
		}

		err = proto.Unmarshal(buf, resMsg)
		if err != nil {
			logging.Infof("client proto unmarshal error %v", err)
			resMsg.Code = storepb.ErrCode_Others
			resMsg.Msg = err.Error()
			res <- resMsg
			return
		}
		logging.Infof("got reply from server %v", resMsg)
		res <- resMsg
	}()
	//}()
}

func (cl *client) Send(ctx context.Context, req *storepb.StoreRequest, s network.Stream) error {
	//resMsg := new(storepb.StoreResponse)

	writer := ggio.NewFullWriter(s)
	err := writer.WriteMsg(req)
	if err != nil {
		return err
	}

	logging.Info("client finish write to stream")

	// go func() {
	// 	//defer s.Close()
	// 	time.Sleep(time.Second)
	// 	logging.Info("client start to read reply")
	// 	buf, err := ioutil.ReadAll(s)
	// 	if err != nil {
	// 		return
	// 	}

	// 	err = proto.Unmarshal(buf, resMsg)
	// 	if err != nil {
	// 		return
	// 	}
	// }()
	return nil

}

func (cl *client) Put(key string, value []byte) error {
	err := cl.ConnectTarget()
	if err != nil {
		return err
	}
	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return err
	}

	req := &storepb.StoreRequest{
		Key:    key,
		Value:  value,
		Action: storepb.Action_Put,
	}

	writer := ggio.NewFullWriter(s)
	err = writer.WriteMsg(req)
	if err != nil {
		return err
	}

	logging.Info("client finish write to stream")
	// keep := make(chan struct{})

	// go func() error {
	// 	defer func() {
	// 		keep <- struct{}{}
	// 	}()
	// 	defer s.Close()
	// 	_ = s.SetReadDeadline(time.Now().Add(time.Second * 2))
	// 	logging.Info("client start to read reply")
	// 	buf, err := ioutil.ReadAll(s)
	// 	if err != nil {
	// 		logging.Info(err)
	// 		return err
	// 	}
	// 	resMsg := &storepb.StoreResponse{}
	// 	err = proto.Unmarshal(buf, resMsg)
	// 	if err != nil {
	// 		logging.Info(err)
	// 		return err
	// 	}
	// 	logging.Info(resMsg)
	// 	return nil
	// }()
	// <-keep
	// err = cl.Send(cl.ctx, req, s)
	// if err != nil {
	// 	return err
	// }
	// if res.GetCode() != storepb.ErrCode_None {
	// 	return xerrors.Errorf("%v", res.GetMsg())
	// }
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

	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return nil, err
	}

	req := &storepb.StoreRequest{
		Key:    key,
		Action: storepb.Action_Get,
		Id:     cl.nextId(),
	}

	writer := ggio.NewFullWriter(s)
	err = writer.WriteMsg(req)
	if err != nil {
		return nil, err
	}

	logging.Info("client finish write to stream")

	resC := make(chan *storepb.StoreResponse)
	cl.taskMap[req.Id] = resC

	// res := make(chan *storepb.StoreResponse)
	// cl.SendMessage(cl.ctx, req, res)

	select {
	case <-cl.ctx.Done():
		return nil, cl.ctx.Err()
	case <-time.After(time.Second):
		return nil, xerrors.Errorf("time out")
	case resMsg := <-resC:
		if resMsg.GetCode() != storepb.ErrCode_None {
			if resMsg.GetCode() == storepb.ErrCode_NotFound {
				return nil, ds.ErrNotFound
			}
			return nil, xerrors.New(resMsg.GetMsg())
		}
		return resMsg.GetValue(), nil
	}
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
