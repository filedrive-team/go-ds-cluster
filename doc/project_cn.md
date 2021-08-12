## 项目说明
go-ds-cluster 来自于 Filedrive 团队对 FileDAG (一个基于 IPFS 提供商业化存储服务的项目) 的开发过程中。FileDAG 需要设计 PB 级别的存储服务，单体的 datastore 难以胜任。基于 IPFS 的集群方案比较少，我们知道的有 [ipfs-cluster](https://github.com/ipfs/ipfs-cluster)，但是我们认为 ipfs-cluster 缺少数据分片能力，对数据的管理能力不足，并且我们更倾向于在 datastore 层来建立集群，而不是在更高的 ipfs 层。出于以上的考虑，Filedrive 团队开始创建 go-ds-cluster 来探索在 datastore 层建立集群的方案。

## 项目依赖

go-ds-cluser 会基于一些成熟的开源集群方案来展开。
- 数据分片 - 采用 redis-cluster 使用的 hash slots
- 节点间通信 - 采用 libp2p
- 共识机制 - CRDT 或者 Raft


## 集群架构

  