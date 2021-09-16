package clusterclient

import (
	"context"
	"testing"
)

var c1cfg = `
{
    "identity": {
        "peer_id":"12D3KooWKarkPBVnpHoapeNmxHc654TBP17zNf8wjp1DvK7jr8mn",
        "sk":"CAESQDq0DDsfyoqSI0ZYAMU2FErig+RdeE7EUjMhRNwFDevdkR8NrRFgtiZK/1TziUEWgXT+pmoEPNvBm5FmNBE+4yk="
    },
    "adresses": {
        "swarm": ["/ip4/0.0.0.0/tcp/9680", "/ip4/0.0.0.0/udp/9680/quic"]
    },
    "conf_path": "",
    "nodes": [
        {"id":"12D3KooWM1dWYafTFGJc6Kq5XYX6RRbQTCbZ58kXFWsjdHREJtCB","slots":{"start":0,"end":5460},"swarm":["/ip4/0.0.0.0/tcp/9690"]},
        {"id":"12D3KooWQHrRAyak1wYc9u27Tu9HnAqkafdWhZkaohSP834igaiZ","slots":{"start":5461,"end":10922},"swarm":["/ip4/0.0.0.0/tcp/9691"]},
        {"id":"12D3KooWQWLzFTEE9XD2oZph4UifRkE4BWiapsqHjWMnB1R5WRtS","slots":{"start":10923,"end":16383},"swarm":["/ip4/0.0.0.0/tcp/9692"]}
    ]
}
`
var srv1cfg = `
{
    "identity": {
        "peer_id":"12D3KooWM1dWYafTFGJc6Kq5XYX6RRbQTCbZ58kXFWsjdHREJtCB",
        "sk":"CAESQAuZ4TCeDyMU5CePnvpSoXQ4wc+mjfOHvZAOcOdmVLKCplNKqi7nJ5vnn4kfgnkrVdZi0uGHni0H3eItuFXM7iw="
    },
    "adresses": {
        "swarm": ["/ip4/0.0.0.0/tcp/9690", "/ip4/0.0.0.0/udp/9690/quic"]
    },
    "conf_path": "",
    "nodes": [
        {"id":"12D3KooWM1dWYafTFGJc6Kq5XYX6RRbQTCbZ58kXFWsjdHREJtCB","slots":{"start":0,"end":5460},"swarm":["/ip4/0.0.0.0/tcp/9690"]},
        {"id":"12D3KooWQHrRAyak1wYc9u27Tu9HnAqkafdWhZkaohSP834igaiZ","slots":{"start":5461,"end":10922},"swarm":["/ip4/0.0.0.0/tcp/9691"]},
        {"id":"12D3KooWQWLzFTEE9XD2oZph4UifRkE4BWiapsqHjWMnB1R5WRtS","slots":{"start":10923,"end":16383},"swarm":["/ip4/0.0.0.0/tcp/9692"]}
    ]
}
`
var srv2cfg = `
{
    "identity": {
        "peer_id":"12D3KooWQHrRAyak1wYc9u27Tu9HnAqkafdWhZkaohSP834igaiZ",
        "sk":"CAESQCI0tvxduyFB+S7+YACHiVb91ywGRsahgFCE/s8z2OyU1w49YyaAS2hl/eS/+POy8TIgbYMj+fye7d0ePPHQpZY="
    },
    "adresses": {
        "swarm": ["/ip4/0.0.0.0/tcp/9691", "/ip4/0.0.0.0/udp/9691/quic"]
    },
    "conf_path": "",
    "nodes": [
        {"id":"12D3KooWM1dWYafTFGJc6Kq5XYX6RRbQTCbZ58kXFWsjdHREJtCB","slots":{"start":0,"end":5460},"swarm":["/ip4/0.0.0.0/tcp/9690"]},
        {"id":"12D3KooWQHrRAyak1wYc9u27Tu9HnAqkafdWhZkaohSP834igaiZ","slots":{"start":5461,"end":10922},"swarm":["/ip4/0.0.0.0/tcp/9691"]},
        {"id":"12D3KooWQWLzFTEE9XD2oZph4UifRkE4BWiapsqHjWMnB1R5WRtS","slots":{"start":10923,"end":16383},"swarm":["/ip4/0.0.0.0/tcp/9692"]}
    ]
}
`
var srv3cfg = `
{
    "identity": {
        "peer_id":"12D3KooWQWLzFTEE9XD2oZph4UifRkE4BWiapsqHjWMnB1R5WRtS",
        "sk":"CAESQKQkW+gjZBPJKTIpJzAw3GSakg9px2VmkoH8rQu2ZSTm2kGDmCDwAR3ZaXb18+auXCfTwrg+kYAf+Ie6y+fL+s8="
    },
    "adresses": {
        "swarm": ["/ip4/0.0.0.0/tcp/9692", "/ip4/0.0.0.0/udp/9692/quic"]
    },
    "conf_path": "",
    "nodes": [
        {"id":"12D3KooWM1dWYafTFGJc6Kq5XYX6RRbQTCbZ58kXFWsjdHREJtCB","slots":{"start":0,"end":5460},"swarm":["/ip4/0.0.0.0/tcp/9690"]},
        {"id":"12D3KooWQHrRAyak1wYc9u27Tu9HnAqkafdWhZkaohSP834igaiZ","slots":{"start":5461,"end":10922},"swarm":["/ip4/0.0.0.0/tcp/9691"]},
        {"id":"12D3KooWQWLzFTEE9XD2oZph4UifRkE4BWiapsqHjWMnB1R5WRtS","slots":{"start":10923,"end":16383},"swarm":["/ip4/0.0.0.0/tcp/9692"]}
    ]
}
`

func TestClusterClient(t testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
}
