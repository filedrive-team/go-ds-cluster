package shard

import "github.com/howeyc/crc16"

const (
	SLOTS_NUM = 1 << 14
)

func CRC16Sum(key string) uint16 {
	return crc16.Checksum([]byte(key), crc16.IBMTable)
}
