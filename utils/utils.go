package utils

import (
	"fmt"
	mrand "math/rand"
	"time"
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
