go-ds-cluster
============
> Gathering distributed key-value datastores to become a cluster


<!-- ABOUT THE PROJECT -->
## About The Project

This project is going to implement [go-datastore](https://github.com/ipfs/go-datastore) in a form of cluster.

[IPFS](https://github.com/ipfs/ipfs) is awsome, we like to use ipfs for data sharing. And [IPFS](https://github.com/ipfs/ipfs) using implementation of [go-datastore](https://github.com/ipfs/go-datastore) as base storage.

There are several implementation of [go-datastore](https://github.com/ipfs/go-datastore):
- [go-ds-flatfs](https://github.com/ipfs/go-ds-flatfs)
- [go-ds-badger](https://github.com/ipfs/go-ds-badger)
- [go-ds-leveldb](https://github.com/ipfs/go-ds-leveldb)
- ...

They are mainly focus on storing data within one PC. Single PC has limit on I/O, there lacks a way to take advantage of distributed system with several or more PCs.

We knew [ipfs-cluster](https://github.com/ipfs/ipfs-cluster), it offers a way to manage pin-set within multiple peers. But we thought it is more suitable for manage data backups and recovery. We expect that even only one ipfs peer could also take advantage of distributed storage.

## Architecture

- data node has two basic components
  - datastore offering key-value strorage
  - cluster instance maintains cluster related logic
- any node in the cluster can be used has ipfs datastore, sharding data to data-nodes in the cluster
- a key-value table of all data in the datastore has been maintained by every node
- the relation between hash slots and data-node also been maintained by every node

## Roadmap

- data sharding and hash slots maintaining 
- communication module based on libp2p between data nodes
- consensus module build up
- hash slots re-allocate and re-balance strategy 
- data migration after hash slots re-allocate or re-balance to support dynamically adding or removing nodes
- authentication and data management



<!-- GETTING STARTED -->
## Getting Started

#### Generate config files

```shell
# build config json generator
make dscfg

# generate config json files for 3 server nodes cluster
./dscfg cluster --cluster-node-number=3 [output-dir]
ls [outpu-dir]
# cluster_00.json cluster_01.json cluster_02.json
```

#### Setup server nodes of the cluster
```shell
make dscluster

# copy to directory where to keep the executable binary and config for serv1
cp dscluster [srv1-dir]
cp [output-dir]/cluster_00.json [srv1-dir]/config.json
cd [srv1-dir]

# using flatfs as datastore
./dscluster --conf=[srv1cfg-dir]
# or using mongods as datastore
# ./dscluster --conf=[srv1cfg-dir] --mongodb="mongodb://localhost:27017" 

cp dscluster [srv2-dir]
cp [output-dir]/cluster_01.json [srv2-dir]/config.json
cd [srv2-dir]
./dscluster --conf=[srv2cfg-dir]

cp dscluster [srv3-dir]
cp [output-dir]/cluster_02.json [srv3-dir]/config.json
cd [srv3-dir]
./dscluster --conf=[srv3cfg-dir]
```

#### Embed into ipfs as a plugin

[read about ipfs preloaded-plugins](https://github.com/ipfs/go-ipfs/blob/master/docs/plugins.md#preloaded-plugins)

```shell
# download go-ipfs repo
git clone git@github.com:ipfs/go-ipfs.git
cd go-ipfs

# Add the plugin to the preload list: plugin/loader/preload_list
echo "clusterds github.com/filedrive-team/go-ds-cluster/ipfsplugin *" >> plugin/loader/preload_list
make build
```
Config customed ipfs

```shell
# set ipfs path
export IPFS_PATH=~/.ipfs-dscluster
./ipfs init 
cd ~/.ipfs-dscluster
# modify the "Datasore.Spec" field
vi config
# "Spec": {
#   "mounts": [
#     {
#       "child": {
#         "cfg": "clusterds.json",
#         "type": "clusterds"
#       },
#       "mountpoint": "/blocks",
#       "prefix": "clusterds.datastore",
#       "type": "measure"
#     },
#     {
#       "child": {
#         "compression": "none",
#         "path": "datastore",
#         "type": "levelds"
#       },
#       "mountpoint": "/",
#       "prefix": "leveldb.datastore",
#       "type": "measure"
#     }
#   ],
#   "type": "mount"
# },

# modify the 
echo '{"mounts":[{"cfg":"clusterds.json","mountpoint":"/blocks","type":"clusterds"},{"mountpoint":"/","path":"datastore","type":"levelds"}],"type":"mount"}' > datastore_spec

# generate clusterds.json
/path/to/dscfg client clusterds.json

# finally replace the "nodes" field in clusterds.json
# with really server nodes info 

./ipfs daemon
```

### Prerequisites



### Installation





<!-- USAGE EXAMPLES -->
## Usage




<!-- ROADMAP -->
## Roadmap





<!-- CONTRIBUTING -->
## Contributing

PRs are welcome!



<!-- LICENSE -->
## License

Distributed under the MIT License. 



<!-- CONTACT -->
## Contact




<!-- ACKNOWLEDGEMENTS -->
## Acknowledgements





