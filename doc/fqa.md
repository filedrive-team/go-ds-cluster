How are the number of data nodes, ID of each node and the relationship between hash slots and data nodes been come up ?
```md
- all generated from init script
- use peer id as node id
- hash slots allocated according to the input node number
- will generate config files
```

How to change cluster configs?
```
Currently, can not change.
```

Can cluster accept data when some data nodes is down？
```
Yes, it can. However, error may bump up if the data has been allocated to data node which is down.
```

If all the nodes in the cluster are data nodes?
```
No，there have non-storage nodes which pass data to data node according to config.
```