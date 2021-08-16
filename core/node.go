package core

type DataNodeClient interface {
	Get(key string) (value []byte, err error)
	Has(key string) (exists bool, err error)
	GetSize(key string) (size int, err error)
	Put(key string, value []byte) error
	Delete(key string) error
}
