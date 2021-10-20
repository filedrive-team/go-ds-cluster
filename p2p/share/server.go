package share

import (
	"context"
	"encoding/json"
	"time"

	"github.com/filedrive-team/go-ds-cluster/config"
	"github.com/libp2p/go-libp2p-core/host"
	"github.com/libp2p/go-libp2p-core/network"
	"github.com/libp2p/go-libp2p-core/protocol"
)

const waitClose = 5

type Server struct {
	ctx      context.Context
	host     host.Host
	protocol protocol.ID
	cfg      *config.Config
}

func NewShareServer(ctx context.Context, h host.Host, cfg *config.Config) *Server {
	return &Server{
		ctx:      ctx,
		host:     h,
		protocol: PROTOCOL_V1,
		cfg:      cfg,
	}
}

func (sv *Server) Close() error {
	var err error
	if err = sv.host.Close(); err != nil {
		logging.Error(err)
	}
	return err
}

func (sv *Server) Serve() {
	logging.Info("share node server set stream handler")
	sv.host.SetStreamHandler(sv.protocol, sv.handleStream)
}

func (sv *Server) handleStream(s network.Stream) {
	defer func() {
		logging.Info("waitClose start")
		<-time.After(time.Second * waitClose)
		logging.Info("waitClose end")
		s.Close()
	}()
	logging.Info("serve incoming stream")
	reqMsg := new(ShareRequest)

	if err := ReadRequest(s, reqMsg); err != nil {
		logging.Error(err)
		return
	}

	logging.Infof("req info type %v", reqMsg.Type)
	switch reqMsg.Type {
	case InfoClusterNodes:
		sv.sendClusterInfo(s, reqMsg)
	case InfoIdentity:
		sv.sendIdentity(s, reqMsg)
	default:
		logging.Warnf("unhandled type: %v", reqMsg.Type)
	}
}

func (sv *Server) sendClusterInfo(s network.Stream, req *ShareRequest) {
	res := &ShareReply{
		Type: InfoClusterNodes,
	}
	bs, err := json.Marshal(sv.cfg.Nodes)
	if err != nil {
		res.Code = ErrOthers
		res.Msg = err.Error()
	} else {
		res.Info = bs
	}

	if err := WriteReply(s, res); err != nil {
		logging.Error(err)
	}
}

func (sv *Server) sendIdentity(s network.Stream, req *ShareRequest) {
	res := &ShareReply{
		Type: InfoIdentity,
	}
	if int(req.Index)+1 > len(sv.cfg.IdentityList) {
		res.Code = ErrOthers
		res.Msg = "request identity index out of range"
		if err := WriteReply(s, res); err != nil {
			logging.Error(err)
		}
		return
	}
	bs, err := json.Marshal(sv.cfg.IdentityList[req.Index])
	if err != nil {
		res.Code = ErrOthers
		res.Msg = err.Error()
	} else {
		res.Info = bs
	}

	if err := WriteReply(s, res); err != nil {
		logging.Error(err)
	}
}
