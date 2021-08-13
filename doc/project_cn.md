## 项目说明
go-ds-cluster 来自于 Filedrive 团队对 FileDAG (一个基于 IPFS 提供商业化存储服务的项目) 的开发过程中。FileDAG 需要设计 PB 级别的存储服务，单体的 datastore 难以胜任。基于 IPFS 的集群方案比较少，我们知道的有 [ipfs-cluster](https://github.com/ipfs/ipfs-cluster)，但是我们认为 ipfs-cluster 缺少数据分片能力，对数据的管理能力不足，并且我们更倾向于在 datastore 层来建立集群，而不是在更高的 ipfs 层。出于以上的考虑，Filedrive 团队开始创建 go-ds-cluster 来探索在 datastore 层建立集群的方案。

## 项目依赖

go-ds-cluster 会基于一些成熟的开源集群方案来展开。
- 数据分片 - 采用 redis-cluster 使用的 hash slots
- 节点间通信 - 采用 libp2p
- 共识机制 - CRDT 或者 Raft


## 集群架构

- 数据节点由两个基本部分组成
  - datastore 提供 key-value 存储服务
  - cluster instance 负责处理集群相关业务
- 集群中任意一个节点都可以承担 ipfs 节点的存储服务，把数据按照集群的分片规则分散到集群的不同数据节点
- 集群共同维护一个 key-value 表，代表集群的全部存储数据
- 集群共同维护 hash slots 和 数据节点的关系

## 路线图

- 完成数据分片功能和 hash slots 维护
- 节点间基于 libp2p 的通信模块
- 集群节点间共识机制的完成
- 实现 hash slots 的重新分配和重新平衡
- 实现 hash slots 重新分配平衡后的数据迁移以支持动态添加和删除数据节点
- 权限和数据管理