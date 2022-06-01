package store

import (
	"sync"
)

var (
	reqMsgPool   sync.Pool
	replyMsgPool sync.Pool
)

func init() {
	reqMsgPool.New = func() interface{} {
		return new(RequestMessage)
	}
	replyMsgPool.New = func() interface{} {
		return new(ReplyMessage)
	}
}

func (req *RequestMessage) reset() {
	req.Key = ""
	req.Value = req.Value[:0]
	req.Action = 0
	req.Query.KeysOnly = false
	req.Query.Limit = 0
	req.Query.Offset = 0
	req.Query.Prefix = ""
}

func (rep *ReplyMessage) reset() {
	rep.Code = 0
	rep.Msg = ""
	rep.Value = rep.Value[:0]
	rep.Size = 0
	rep.Exists = false
}
