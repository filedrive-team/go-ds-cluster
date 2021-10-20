package share

import (
	"context"

	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-core/peerstore"
	"github.com/libp2p/go-libp2p-core/protocol"
	"golang.org/x/xerrors"
)

type Client struct {
	ctx      context.Context
	src      host.Host
	target   peer.AddrInfo
	protocol protocol.ID
}

func NewShareClient(ctx context.Context, src host.Host, target peer.AddrInfo, pid protocol.ID) *Client {
	src.Peerstore().AddAddrs(target.ID, target.Addrs, peerstore.PermanentAddrTTL)
	return &Client{
		ctx:      ctx,
		src:      src,
		target:   target,
		protocol: pid,
	}
}

func (cl *Client) Close() error {
	return cl.src.Close()
}

func (cl *Client) IsTargetConnected() bool {
	return cl.src.Network().Connectedness(cl.target.ID) == network.Connected
}

func (cl *Client) ConnectTarget() error {
	if cl.IsTargetConnected() {
		return nil
	}

	return cl.src.Connect(cl.ctx, cl.target)
}

func (cl *Client) GetClusterInfo() (value []byte, err error) {
	_ = cl.ConnectTarget()

	s, err := cl.src.NewStream(cl.ctx, cl.target.ID, cl.protocol)
	if err != nil {
		return nil, err
	}
	defer s.Close()

	req := &ShareRequest{
		Type: InfoClusterNodes,
	}

	if err := WriteRequst(s, req); err != nil {
		logging.Error(err)
		return nil, err
	}

	reply := &ShareReply{}

	if err := ReadReply(s, reply); err != nil {
		logging.Error(err)
		return nil, err
	}
	if reply.Code != ErrNone {
		return nil, xerrors.New(reply.Msg)
	}
	return reply.Info, nil
}
