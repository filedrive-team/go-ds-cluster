package utils

import (
	"fmt"
	mrand "math/rand"
	"time"
)

func RandPort() string {
	mrand.Seed(time.Now().Unix() * int64(mrand.Intn(9999)))
	r := mrand.Float64()
	m := 4000 + 6000*r
	return fmt.Sprintf("%.0f", m)
}
