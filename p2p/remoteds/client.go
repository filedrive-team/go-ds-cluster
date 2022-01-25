package remoteds

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

const default_time_out = 60 * 3

type client struct {
	ctx      context.Context
	src      host.Host
	target   peer.AddrInfo
	protocol protocol.ID
	token    string
	timeout  int
}

func NewStoreClient(ctx context.Context, src host.Host, target peer.AddrInfo, pid protocol.ID, timeout int, token string) core.RemoteDataNodeClient {
	to := timeout
	if to == 0 {
		to = default_time_out
	}
	src.Peerstore().AddAddrs(target.ID, target.Addrs, peerstore.PermanentAddrTTL)
	return &client{
		ctx:      ctx,
		src:      src,
		target:   target,
		protocol: pid,
		timeout:  to,
		token:    token,
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
	if err := cl.ConnectTarget(); err != nil {
		return err
	}

	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return err
	}
	defer s.Close()

	req := &RequestMessage{
		AccessToken: cl.token,
		Key:         key,
		Value:       value,
		Action:      ActPut,
	}
	if err := WriteRequstMsg(s, req, cl.timeout); err != nil {
		logging.Error(err)
		return err
	}

	reply := &ReplyMessage{}

	if err := ReadReplyMsg(s, reply, cl.timeout); err != nil {
		logging.Error(err)
		return err
	}

	if reply.Code != ErrNone {
		return xerrors.New(reply.Msg)
	}
	return nil
}

func (cl *client) TouchFile(key string, value []byte) error {
	if err := cl.ConnectTarget(); err != nil {
		return err
	}

	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return err
	}
	defer s.Close()

	req := &RequestMessage{
		AccessToken: cl.token,
		Key:         key,
		Value:       value,
		Action:      ActTouchFile,
	}
	if err := WriteRequstMsg(s, req, cl.timeout); err != nil {
		logging.Error(err)
		return err
	}

	reply := &ReplyMessage{}

	if err := ReadReplyMsg(s, reply, cl.timeout); err != nil {
		logging.Error(err)
		return err
	}

	if reply.Code != ErrNone {
		return xerrors.New(reply.Msg)
	}
	return nil
}

func (cl *client) Delete(key string) error {
	if err := cl.ConnectTarget(); err != nil {
		return err
	}

	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return err
	}
	defer s.Close()

	req := &RequestMessage{
		AccessToken: cl.token,
		Key:         key,
		Action:      ActDelete,
	}
	if err := WriteRequstMsg(s, req, cl.timeout); err != nil {
		logging.Error(err)
		return err
	}

	reply := &ReplyMessage{}

	if err := ReadReplyMsg(s, reply, cl.timeout); err != nil {
		logging.Error(err)
		return err
	}
	if reply.Code != ErrNone {
		return xerrors.New(reply.Msg)
	}
	return nil
}

func (cl *client) DeleteFile(key string) error {
	if err := cl.ConnectTarget(); err != nil {
		return err
	}

	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return err
	}
	defer s.Close()

	req := &RequestMessage{
		AccessToken: cl.token,
		Key:         key,
		Action:      ActDeleteFile,
	}
	if err := WriteRequstMsg(s, req, cl.timeout); err != nil {
		logging.Error(err)
		return err
	}

	reply := &ReplyMessage{}

	if err := ReadReplyMsg(s, reply, cl.timeout); err != nil {
		logging.Error(err)
		return err
	}
	if reply.Code != ErrNone {
		return xerrors.New(reply.Msg)
	}
	return nil
}

func (cl *client) Get(key string) (value []byte, err error) {
	if err := cl.ConnectTarget(); err != nil {
		return nil, err
	}

	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	req := &RequestMessage{
		AccessToken: cl.token,
		Key:         key,
		Action:      ActGet,
	}

	if err := WriteRequstMsg(s, req, cl.timeout); err != nil {
		logging.Error(err)
		return nil, err
	}

	reply := &ReplyMessage{}

	if err := ReadReplyMsg(s, reply, cl.timeout); err != nil {
		logging.Error(err)
		return nil, err
	}
	if reply.Code != ErrNone {
		if reply.Code == ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, xerrors.New(reply.Msg)
	}
	return reply.Value, nil
}

func (cl *client) FileInfo(key string) (value []byte, err error) {
	if err := cl.ConnectTarget(); err != nil {
		return nil, err
	}

	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	req := &RequestMessage{
		AccessToken: cl.token,
		Key:         key,
		Action:      ActFileInfo,
	}

	if err := WriteRequstMsg(s, req, cl.timeout); err != nil {
		logging.Error(err)
		return nil, err
	}

	reply := &ReplyMessage{}

	if err := ReadReplyMsg(s, reply, cl.timeout); err != nil {
		logging.Error(err)
		return nil, err
	}
	if reply.Code != ErrNone {
		if reply.Code == ErrNotFound {
			return nil, ds.ErrNotFound
		}
		return nil, xerrors.New(reply.Msg)
	}
	return reply.Value, nil
}

func (cl *client) Has(key string) (exists bool, err error) {
	if err := cl.ConnectTarget(); err != nil {
		return false, err
	}
	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return false, err
	}
	defer s.Close()

	req := &RequestMessage{
		AccessToken: cl.token,
		Key:         key,
		Action:      ActHas,
	}
	if err := WriteRequstMsg(s, req, cl.timeout); err != nil {
		logging.Error(err)
		return false, err
	}

	reply := &ReplyMessage{}
	if err := ReadReplyMsg(s, reply, cl.timeout); err != nil {
		logging.Error(err)
		return false, err
	}
	// var b bytes.Buffer
	// if err := reply.MarshalCBOR(&b); err == nil {
	// 	logging.Infof("[Has] reply bytes: %v", b.Bytes())
	// }
	if reply.Code != ErrNone {
		if reply.Code == ErrNotFound {
			return false, nil
		}
		return false, xerrors.New(reply.Msg)
	}

	return reply.Exists, nil
}

func (cl *client) GetSize(key string) (size int, err error) {
	if err := cl.ConnectTarget(); err != nil {
		return -1, err
	}
	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return -1, err
	}
	defer s.Close()

	req := &RequestMessage{
		AccessToken: cl.token,
		Key:         key,
		Action:      ActGetSize,
	}

	if err := WriteRequstMsg(s, req, cl.timeout); err != nil {
		logging.Error(err)
		return -1, err
	}

	reply := &ReplyMessage{}

	if err := ReadReplyMsg(s, reply, cl.timeout); err != nil {
		logging.Error(err)
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
	if err := cl.ConnectTarget(); err != nil {
		return nil, err
	}
	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return nil, err
	}
	//defer s.Close()

	req := &RequestMessage{
		AccessToken: cl.token,
		Query:       P2PQuery(q),
		Action:      ActQuery,
	}

	if err := WriteRequstMsg(s, req, cl.timeout); err != nil {
		logging.Error(err)
		return nil, err
	}

	nextValue := func() (dsq.Result, bool) {
		ent := &QueryResultEntry{}

		if err := ReadQueryResultEntry(s, ent, cl.timeout); err != nil {
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

func (cl *client) ListFiles(prefix string) (chan core.Pair, error) {
	if err := cl.ConnectTarget(); err != nil {
		return nil, err
	}
	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return nil, err
	}

	req := &RequestMessage{
		AccessToken: cl.token,
		Key:         prefix,
		Action:      ActListFiles,
	}

	if err := WriteRequstMsg(s, req, cl.timeout); err != nil {
		logging.Error(err)
		return nil, err
	}
	outchan := make(chan core.Pair)
	go func() {
		defer s.Close()
		defer close(outchan)
		for {
			select {
			case <-cl.ctx.Done():
				return
			default:
				ent := &QueryResultEntry{}

				if err := ReadQueryResultEntry(s, ent, cl.timeout); err != nil {
					logging.Error(err)
					return
				}
				if ent.Code == ErrEOF {
					return
				}
				if ent.Code != ErrNone {
					logging.Error(ent.Msg)
					return
				}
				outchan <- core.Pair{
					Key:   ent.Key,
					Value: ent.Value,
				}
			}
		}
	}()

	return outchan, nil
}
