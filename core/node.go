package core

import (
	dsq "github.com/ipfs/go-datastore/query"
)

type Pair struct {
	Key   string
	Value []byte
}

// DataNode - basic Datastore operations
type DataNode interface {
	Get(key string) (value []byte, err error)
	Has(key string) (exists bool, err error)
	GetSize(key string) (size int, err error)
	Put(key string, value []byte) error
	Delete(key string) error
	Query(q dsq.Query) (dsq.Results, error)
}

// RemoteDataNode
type RemoteDataNode interface {
	TouchFile(key string, value []byte) error
	FileInfo(key string) (value []byte, err error)
	DeleteFile(key string) error
	ListFiles(prefix string) (chan Pair, error)
}

// DataNodeClient abstract data request side
type DataNodeClient interface {
	DataNode

	ConnectTarget() error
	IsTargetConnected() bool
	Close() error
}

// RemoteDataNodeClient abstract data request side
type RemoteDataNodeClient interface {
	DataNode
	RemoteDataNode

	ConnectTarget() error
	IsTargetConnected() bool
	Close() error
}

// DataNodeServer abstract storage side applying request
type DataNodeServer interface {
	Serve()
	Close() error
}
