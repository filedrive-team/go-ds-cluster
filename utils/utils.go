package utils

import (
	"context"
	"fmt"
	mrand "math/rand"
	"path/filepath"
	"time"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-unixfs"
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

func HeadFileNode(ctx context.Context, tcid cid.Cid, dagServ format.DAGService, p string, cb func(string, cid.Cid, error)) {
	nd, err := dagServ.Get(ctx, tcid)
	if err != nil {
		cb("", cid.Undef, err)
		return
	}
	links := nd.Links()
	if len(links) == 0 {
		cb(p, tcid, nil)
		return
	}

	ffn := nd.(*merkledag.ProtoNode)
	fsnd, err := unixfs.FSNodeFromBytes(ffn.Data())
	if err != nil {
		cb("", cid.Undef, err)
		return
	}
	if !fsnd.IsDir() {
		cb(p, tcid, nil)
		return
	}
	firstLink := links[0]
	HeadFileNode(ctx, firstLink.Cid, dagServ, filepath.Join(p, firstLink.Name), cb)
}

func TailFileNode(ctx context.Context, tcid cid.Cid, dagServ format.DAGService, p string, cb func(string, cid.Cid, error)) {
	nd, err := dagServ.Get(ctx, tcid)
	if err != nil {
		cb("", cid.Undef, err)
		return
	}
	links := nd.Links()
	if len(links) == 0 {
		cb(p, tcid, nil)
		return
	}

	ffn := nd.(*merkledag.ProtoNode)
	fsnd, err := unixfs.FSNodeFromBytes(ffn.Data())
	if err != nil {
		cb("", cid.Undef, err)
		return
	}
	if !fsnd.IsDir() {
		cb(p, tcid, nil)
		return
	}
	lastLink := links[len(links)-1]
	TailFileNode(ctx, lastLink.Cid, dagServ, filepath.Join(p, lastLink.Name), cb)
}
