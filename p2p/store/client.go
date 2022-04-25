package store

import (
	"context"
	"sync"

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

const maxStreamNum = 512

type client struct {
	ctx        context.Context
	src        host.Host
	target     peer.AddrInfo
	protocol   protocol.ID
	trans      chan transaction
	queryTrans chan queryTrans
	closeChan  chan struct{}
	close      func()
}

func NewStoreClient(ctx context.Context, src host.Host, target peer.AddrInfo, pid protocol.ID) core.DataNodeClient {
	src.Peerstore().AddAddrs(target.ID, target.Addrs, peerstore.PermanentAddrTTL)
	cl := &client{
		ctx:        ctx,
		src:        src,
		target:     target,
		protocol:   pid,
		trans:      make(chan transaction, maxStreamNum),
		closeChan:  make(chan struct{}),
		queryTrans: make(chan queryTrans),
	}
	if err := cl.ConnectTarget(); err != nil {
		panic(err)
	}
	for i := 0; i < maxStreamNum; i++ {
		go cl.initStream()
	}
	go cl.initQueryStream()
	var once sync.Once
	cl.close = func() {
		once.Do(func() {
			close(cl.closeChan)
		})
	}
	return cl
}

func (cl *client) initStream() {
	var s network.Stream
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	for {
		select {
		case <-cl.ctx.Done():
			return
		case <-cl.closeChan:
			return
		case tm := <-cl.trans:
			func() {
				var err error
				defer func() {
					if err != nil {
						tm.Out <- &ReplyMessage{
							Code: ErrOthers,
							Msg:  err.Error(),
						}
					}
				}()
				if s == nil {
					s, err = cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
					if err != nil {
						logging.Errorf("client init stream failed: %s", err)
						return
					}
				}
				if err = WriteRequstMsg(s, tm.In); err != nil {
					logging.Errorf("%s write request failed: %s", tm.In.Action, err)
					return
				}
				reply := &ReplyMessage{}

				if err = ReadReplyMsg(s, reply); err != nil {
					logging.Errorf("%s read reply failed: %s", tm.In.Action, err)
					return
				}
				tm.Out <- reply
			}()
		}
	}
}

func (cl *client) initQueryStream() {
	var s network.Stream
	defer func() {
		if s != nil {
			s.Close()
		}
	}()

	for {
		select {
		case <-cl.ctx.Done():
			return
		case <-cl.closeChan:
			return
		case tm := <-cl.queryTrans:
			func() {
				var err error
				defer func() {
					if err != nil {
						tm.Out <- &QueryResultEntry{
							Code: ErrOthers,
							Msg:  err.Error(),
						}
					}
				}()
				if s == nil {
					s, err = cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
					if err != nil {
						logging.Errorf("client init stream failed(query): %s", err)
						return
					}
				}
				if err = WriteRequstMsg(s, tm.In); err != nil {
					logging.Errorf("%s write request failed: %s", tm.In.Action, err)
					return
				}
				for {
					select {
					case <-tm.Stop:
						return
					case <-cl.ctx.Done():
						return
					case <-cl.closeChan:
						return
					default:
						ent := &QueryResultEntry{}
						if err = ReadQueryResultEntry(s, ent); err != nil {
							logging.Errorf("%s read query result failed: %s", tm.In.Action, err)
							return
						}
						tm.Out <- ent
					}
				}

			}()
		}
	}
}

func (cl *client) Close() error {
	cl.close()
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
	req := &RequestMessage{
		Key:    key,
		Value:  value,
		Action: ActPut,
	}
	replyChan := make(chan *ReplyMessage)
	cl.trans <- transaction{
		In:  req,
		Out: replyChan,
	}

	reply := <-replyChan
	if reply.Code != ErrNone {
		return xerrors.New(reply.Msg)
	}
	return nil
}

func (cl *client) Delete(key string) error {
	req := &RequestMessage{
		Key:    key,
		Action: ActDelete,
	}
	replyChan := make(chan *ReplyMessage)
	cl.trans <- transaction{
		In:  req,
		Out: replyChan,
	}

	reply := <-replyChan
	if reply.Code != ErrNone {
		return xerrors.New(reply.Msg)
	}
	return nil
}

func (cl *client) Get(key string) (value []byte, err error) {
	req := &RequestMessage{
		Key:    key,
		Action: ActGet,
	}
	replyChan := make(chan *ReplyMessage)
	cl.trans <- transaction{
		In:  req,
		Out: replyChan,
	}

	reply := <-replyChan
	if reply.Code != ErrNone {
		if reply.Code == ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, xerrors.New(reply.Msg)
	}
	return reply.Value, nil
}

func (cl *client) Has(key string) (exists bool, err error) {
	req := &RequestMessage{
		Key:    key,
		Action: ActHas,
	}
	replyChan := make(chan *ReplyMessage)
	cl.trans <- transaction{
		In:  req,
		Out: replyChan,
	}

	reply := <-replyChan
	if reply.Code != ErrNone {
		if reply.Code == ErrNotFound {
			return false, nil
		}
		return false, xerrors.New(reply.Msg)
	}

	return reply.Exists, nil
}

func (cl *client) GetSize(key string) (size int, err error) {
	req := &RequestMessage{
		Key:    key,
		Action: ActGetSize,
	}
	replyChan := make(chan *ReplyMessage)
	cl.trans <- transaction{
		In:  req,
		Out: replyChan,
	}

	reply := <-replyChan
	if reply.Code != ErrNone {
		if reply.Code == ErrNotFound {
			return -1, ds.ErrNotFound
		}
		return -1, xerrors.New(reply.Msg)
	}

	return int(reply.Size), nil
}

func (cl *client) Query(q dsq.Query) (dsq.Results, error) {
	req := &RequestMessage{
		Query:  P2PQuery(q),
		Action: ActQuery,
	}
	replyChan := make(chan *QueryResultEntry)
	stopChan := make(chan struct{})
	cl.queryTrans <- queryTrans{
		In:   req,
		Out:  replyChan,
		Stop: stopChan,
	}

	nextValue := func() (dsq.Result, bool) {
		ent := <-replyChan
		if ent.Code != ErrNone {
			if ent.Code == ErrQueryResultEnd {
				return dsq.Result{}, false
			}
			return dsq.Result{Error: xerrors.New(ent.Msg)}, false
		}
		return dsq.Result{Entry: dsq.Entry{
			Key:   ent.Key,
			Value: ent.Value,
		}}, true
	}
	var once sync.Once
	close := func() {
		once.Do(func() {
			close(stopChan)
		})
	}
	return dsq.ResultsFromIterator(q, dsq.Iterator{
		Close: func() error {
			close()
			return nil
		},
		Next: nextValue,
	}), nil
}

type transaction struct {
	In  *RequestMessage
	Out chan *ReplyMessage
}

type queryTrans struct {
	In   *RequestMessage
	Out  chan *QueryResultEntry
	Stop chan struct{}
}
