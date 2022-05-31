package store

import (
	"context"

	"github.com/filedrive-team/go-ds-cluster/core"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"golang.org/x/xerrors"
)

type client struct {
	ctx      context.Context
	src      host.Host
	target   peer.AddrInfo
	protocol protocol.ID
}

func NewStoreClient(ctx context.Context, src host.Host, target peer.AddrInfo, pid protocol.ID) core.DataNodeClient {
	src.Peerstore().AddAddrs(target.ID, target.Addrs, peerstore.PermanentAddrTTL)
	return &client{
		ctx:      ctx,
		src:      src,
		target:   target,
		protocol: pid,
	}
}

func (cl *client) Close() error {
	return cl.src.Close()
}

func (cl *client) IsTargetConnected() bool {
	return cl.src.Network().Connectedness(cl.target.ID) == network.Connected
}

func (cl *client) ConnectTarget() error {
	if cl.IsTargetConnected() {
		return nil
	}

	return cl.src.Connect(cl.ctx, cl.target)
}

func (cl *client) Put(key string, value []byte) error {
	_ = cl.ConnectTarget()

	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return err
	}
	defer s.Close()

	// req := &RequestMessage{
	// 	Key:    key,
	// 	Value:  value,
	// 	Action: ActPut,
	// }
	req := reqMsgPool.Get().(*RequestMessage)
	defer reqMsgPool.Put(req)
	req.Key = key
	req.Value = value
	req.Action = ActPut
	if err := WriteRequstMsg(s, req); err != nil {
		logging.Errorf("Put write request failed: %s", err)
		return err
	}

	//reply := &ReplyMessage{}
	reply := replyMsgPool.Get().(*ReplyMessage)
	reply.reset()
	defer replyMsgPool.Put(reply)
	if err := ReadReplyMsg(s, reply); err != nil {
		logging.Errorf("Put read reply failed: %s", err)
		return err
	}
	// var b bytes.Buffer
	// if err := reply.MarshalCBOR(&b); err == nil {
	// 	logging.Infof("[Put] reply bytes: %v", b.Bytes())
	// }
	if reply.Code != ErrNone {
		return xerrors.New(reply.Msg)
	}
	return nil
}

func (cl *client) Delete(key string) error {
	_ = cl.ConnectTarget()

	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return err
	}
	defer s.Close()

	// req := &RequestMessage{
	// 	Key:    key,
	// 	Action: ActDelete,
	// }
	req := reqMsgPool.Get().(*RequestMessage)
	req.reset()
	defer reqMsgPool.Put(req)
	req.Key = key
	req.Action = ActDelete
	if err := WriteRequstMsg(s, req); err != nil {
		logging.Errorf("Delete write request failed: %s", err)
		return err
	}

	//reply := &ReplyMessage{}
	reply := replyMsgPool.Get().(*ReplyMessage)
	reply.reset()
	defer replyMsgPool.Put(reply)
	if err := ReadReplyMsg(s, reply); err != nil {
		logging.Errorf("Delete read reply failed: %s", err)
		return err
	}
	if reply.Code != ErrNone {
		return xerrors.New(reply.Msg)
	}
	return nil
}

func (cl *client) Get(key string) (value []byte, err error) {
	_ = cl.ConnectTarget()

	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	// req := &RequestMessage{
	// 	Key:    key,
	// 	Action: ActGet,
	// }
	req := reqMsgPool.Get().(*RequestMessage)
	req.reset()
	defer reqMsgPool.Put(req)
	req.Key = key
	req.Action = ActGet

	if err := WriteRequstMsg(s, req); err != nil {
		logging.Errorf("Get write request failed: %s", err)
		return nil, err
	}

	//reply := &ReplyMessage{}
	reply := replyMsgPool.Get().(*ReplyMessage)
	reply.reset()
	defer replyMsgPool.Put(reply)
	if err := ReadReplyMsg(s, reply); err != nil {
		logging.Errorf("Get read reply failed: %s", err)
		return nil, err
	}
	if reply.Code != ErrNone {
		if reply.Code == ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, xerrors.New(reply.Msg)
	}
	value = make([]byte, len(reply.Value))
	copy(value, reply.Value)
	return
}

func (cl *client) Has(key string) (exists bool, err error) {
	_ = cl.ConnectTarget()
	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return false, err
	}
	defer s.Close()

	// req := &RequestMessage{
	// 	Key:    key,
	// 	Action: ActHas,
	// }
	req := reqMsgPool.Get().(*RequestMessage)
	req.reset()
	defer reqMsgPool.Put(req)
	req.Key = key
	req.Action = ActHas
	if err := WriteRequstMsg(s, req); err != nil {
		logging.Errorf("Has write request failed: %s", err)
		return false, err
	}

	//reply := &ReplyMessage{}
	reply := replyMsgPool.Get().(*ReplyMessage)
	reply.reset()
	defer replyMsgPool.Put(reply)
	if err := ReadReplyMsg(s, reply); err != nil {
		logging.Errorf("Has read reply failed: %s", err)
		return false, err
	}

	if reply.Code != ErrNone {
		if reply.Code == ErrNotFound {
			return false, nil
		}
		return false, xerrors.New(reply.Msg)
	}

	return reply.Exists, nil
}

func (cl *client) GetSize(key string) (size int, err error) {
	_ = cl.ConnectTarget()
	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return -1, err
	}
	defer s.Close()

	// req := &RequestMessage{
	// 	Key:    key,
	// 	Action: ActGetSize,
	// }
	req := reqMsgPool.Get().(*RequestMessage)
	req.reset()
	defer reqMsgPool.Put(req)
	req.Key = key
	req.Action = ActGetSize
	if err := WriteRequstMsg(s, req); err != nil {
		logging.Errorf("GetSize write request failed: %s", err)
		return -1, err
	}

	//reply := &ReplyMessage{}
	reply := replyMsgPool.Get().(*ReplyMessage)
	reply.reset()
	defer replyMsgPool.Put(reply)

	if err := ReadReplyMsg(s, reply); err != nil {
		logging.Errorf("GetSize read reply failed: %s", err)
		return -1, err
	}
	if reply.Code != ErrNone {
		if reply.Code == ErrNotFound {
			return -1, ds.ErrNotFound
		}
		return -1, xerrors.New(reply.Msg)
	}

	return int(reply.Size), nil
}

func (cl *client) Query(q dsq.Query) (dsq.Results, error) {
	_ = cl.ConnectTarget()
	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return nil, err
	}
	//defer s.Close()

	// req := &RequestMessage{
	// 	Query:  P2PQuery(q),
	// 	Action: ActQuery,
	// }
	req := reqMsgPool.Get().(*RequestMessage)
	req.reset()
	defer reqMsgPool.Put(req)
	req.Query = P2PQuery(q)
	req.Action = ActQuery

	if err := WriteRequstMsg(s, req); err != nil {
		logging.Error(err)
		return nil, err
	}

	nextValue := func() (dsq.Result, bool) {
		ent := &QueryResultEntry{}

		if err := ReadQueryResultEntry(s, ent); err != nil {
			s.Close()
			return dsq.Result{Error: err}, false
		}
		if ent.Code != ErrNone {
			s.Close()
			return dsq.Result{Error: xerrors.New(ent.Msg)}, false
		}
		return dsq.Result{Entry: dsq.Entry{
			Key:   ent.Key,
			Value: ent.Value,
		}}, true
	}

	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Close: func() error {
			return s.Close()
		},
		Next: nextValue,
	}), nil
}
