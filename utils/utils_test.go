package utils

import (
	"strconv"
	"testing"
)

func TestRandPort(t *testing.T) {
	port := RandPort()
	if port == "" {
		t.Fatal("empty port number")
	}
	p, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal("port convert to int failed")
	}
	if p < RandPortMin || p > RandPortMax {
		t.Fatal("unexpected port number")
	}
}
