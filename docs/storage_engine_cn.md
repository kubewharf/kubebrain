# 存储引擎

## 对存储引擎的特性要求

当前我们对存储引擎有以下特性要求

- 支持快照读
- 支持双向遍历
- 支持读写事务或者带有CAS功能的写事务
- 对外暴露逻辑时钟

对存储引擎的事务保证，具有以下级别要求

- Isolation Guarantee: Snapshot Isolation
- Session Guarantee: Linearizable

针对[HATs](http://www.vldb.org/pvldb/vol7/p181-bailis.pdf) 中总结的Snapshot Isolation可能的异常现象

- Write Skew: 目前的数据结构和写入方式设计理论上不会破坏一致性约束
- Phantom: 采用Snapshot Read，理论上提前读取到并发事务写入的值不影响数据最终一致性

## 当前版本的存储引擎接口定义

存储引擎抽象为[接口定义](../pkg/storage/interface.go)，在未来可能会进行进一步优化，目标是使得其能够适配更多的KV数据库
