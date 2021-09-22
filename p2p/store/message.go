package store

import dsq "github.com/ipfs/go-datastore/query"

type Act uint8

const (
	ActPut Act = 1 + iota
	ActDelete
	ActGet
	ActGetSize
	ActHas
	ActQuery
)

type ErrCode uint8

const (
	ErrNone ErrCode = iota
	ErrNotFound

	ErrOthers = 100
)

type RequestMessage struct {
	Key    string
	Value  []byte
	Query  Query
	Action Act
}

type ReplyMessage struct {
	Code   ErrCode
	Msg    string
	Value  []byte
	Size   int64
	Exists bool
}

type Query struct {
	Prefix   string
	Limit    int64
	Offset   int64
	KeysOnly bool
}

type QueryResultEntry struct {
	Code  ErrCode
	Msg   string
	Key   string
	Value []byte
	Size  int64
}

func DSQuery(q Query) dsq.Query {
	return dsq.Query{
		Prefix:   q.Prefix,
		Limit:    int(q.Limit),
		Offset:   int(q.Offset),
		KeysOnly: q.KeysOnly,
	}
}
func P2PQuery(q dsq.Query) Query {
	return Query{
		Prefix:   q.Prefix,
		Limit:    int64(q.Limit),
		Offset:   int64(q.Offset),
		KeysOnly: q.KeysOnly,
	}
}
