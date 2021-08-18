package main

import (
	"fmt"
	"os"

	"github.com/filedrive-team/go-ds-cluster/p2p/store"
	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := gen.WriteTupleEncodersToFile("./p2p/store/cbor_gen.go", "store",
		store.RequestMessage{},
		store.ReplyMessage{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

}
