package store

type Act uint8

const (
	ActPut Act = 1 + iota
	ActDelete
	ActGet
	ActGetSize
	ActHas
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
	Action Act
}

type ReplyMessage struct {
	Code   ErrCode
	Msg    string
	Value  []byte
	Size   int64
	Exists bool
}
