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

func NewStoreServer(ctx context.Context, h host.Host, pid protocol.ID, cfg *config.Config) *Server {
	return &Server{
		ctx:      ctx,
		host:     h,
		protocol: pid,
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
	logging.Info("server incoming stream")
	reqMsg := new(ShareRequest)

	if err := ReadRequest(s, reqMsg); err != nil {
		logging.Error(err)
		return
	}

	logging.Infof("req info type %v", reqMsg.Type)
	switch reqMsg.Type {
	case InfoClusterNodes:
		sv.sendClusterInfo(s, reqMsg)
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
