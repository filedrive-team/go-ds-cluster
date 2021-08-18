package core

type DataNode interface {
	Get(key string) (value []byte, err error)
	Has(key string) (exists bool, err error)
	GetSize(key string) (size int, err error)
	Put(key string, value []byte) error
	Delete(key string) error
}

type DataNodeClient interface {
	DataNode

	ConnectTarget() error
	IsTargetConnected() bool
	SetHandle()
	Close() error
}

type DataNodeServer interface {
	Serve()
	Close() error
}
