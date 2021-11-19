package main

import (
	"fmt"
	"os"

	"github.com/filedrive-team/go-ds-cluster/p2p/remoteds"
	"github.com/filedrive-team/go-ds-cluster/p2p/share"
	"github.com/filedrive-team/go-ds-cluster/p2p/store"
	gen "github.com/whyrusleeping/cbor-gen"
)

func main() {
	err := gen.WriteTupleEncodersToFile("./p2p/store/cbor_gen.go", "store",
		store.RequestMessage{},
		store.ReplyMessage{},
		store.QueryResultEntry{},
		store.Query{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = gen.WriteTupleEncodersToFile("./p2p/share/cbor_gen.go", "share",
		share.ShareRequest{},
		share.ShareReply{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = gen.WriteTupleEncodersToFile("./p2p/remoteds/cbor_gen.go", "remoteds",
		remoteds.RequestMessage{},
		remoteds.ReplyMessage{},
		remoteds.QueryResultEntry{},
		remoteds.Query{},
	)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
