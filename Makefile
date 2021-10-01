.PHONY: dscluster
dscluster:
	go build -o ./dscluster ./cmd/dscluster/

.PHONY: dscfg
dscfg:
	go build -o ./dscfg ./cmd/dscfg/

.PHONY: cborgen
cborgen:
	rm -f ./p2p/store/cbor_gen.go
	go run ./gen/main.go

.PHONY: dsclient
dsclient:
	go build -o ./dsclient ./cmd/dsclient