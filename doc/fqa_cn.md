集群中的数据节点数量、ID、以及和 hash slots 的对应关系是如何生成？
```md
- 使用初始化脚本来生成
- 根据输入的节点数量来生成相应数量的ID, 计划使用 peer id
- 根据节点数量来分配 hash slots
- 生成配置文件
```

集群配置如何更改？
```
暂不支持更改
```

集群中节点未全部启动好时，可以接受存储服务吗？
```
可以接受存储服务，但是如果待存储的数据被分配到未启动的节点时，会收到报错。
```

集群中的节点全部是数据节点吗？
```
不是，有非存储节点，只负责根据配置把待存储的数据分流到对应的数据节点上。
```