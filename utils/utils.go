package utils

import (
	"fmt"
	mrand "math/rand"
	"time"

	"github.com/sigurn/crc8"
)

const (
	RandPortMin = 4000
	RandPortMax = 10000
)

func RandPort() string {
	mrand.Seed(time.Now().Unix() * int64(mrand.Intn(RandPortMax)))
	r := mrand.Float64()
	m := RandPortMin + (RandPortMax-RandPortMin)*r
	return fmt.Sprintf("%.0f", m)
}

func CRC8code(k string) uint8 {
	table := crc8.MakeTable(crc8.CRC8_MAXIM)
	return crc8.Checksum([]byte(k), table)
}
