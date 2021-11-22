package main

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/filedrive-team/go-ds-cluster/remoteclient"
)

func main() {
	ctx := context.Background()
	client, err := remoteclient.NewClient(ctx, &remoteclient.Config{
		AccessToken: "",
		Bucket:      "test02",
		Timeout:     2,
		SwarmKey:    "/Users/lifeng/.filejoy/swarm.key",
		Target:      "/ip4/127.0.0.1/tcp/17073/p2p/12D3KooWRLrrvysm9wRSeXoHyyaxh5SDQ1Q1asgYQufrBbN46JwC",
	})
	if err != nil {
		panic(err)
	}
	content := "Better World, Better Life"
	fname := "test-file"
	objname := "fd2.png"
	meta, err := client.AddReader(strings.NewReader(content), int64(len([]byte(content))), fname, objname)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%#v\n", *meta)
	meta2, err := client.FileInfo(objname)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%#v\n", *meta2)
	r, err := client.Get(objname)
	if err != nil {
		panic(err)
	}
	defer r.Close()
	b, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", b)

	r2, err := client.GetByCid(meta2.Cid)
	if err != nil {
		panic(err)
	}
	defer r2.Close()
	b2, err := ioutil.ReadAll(r2)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", b2)

	if !bytes.Equal(b, b2) {
		panic("data not match")
	}
}
