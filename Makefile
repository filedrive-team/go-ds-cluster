.PHONY: dscluster
dscluster:
	go build -o ./dscluster ./cmd/dscluster/

.PHONY: dscfg
dscfg:
	go build -o ./dscfg ./cmd/dscfg/