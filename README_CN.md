# KubeBrain

[English](README.md) | 中文

分布式应用编排调度系统kubernetes已经成为云原生应用基座的事实标准，但是其官方的稳定运行规模仅仅局限在5K节点的规模；这对于大部分的应用场景已经足够，但是对于百万规模机器节点的应用场景，在规模上还是不够。尤其随着“数字化”，”云原生化”的发展，全球整体的IT基础设施规模仍然会加速增长，对于分布式应用编排调度系统，有两种方式来适应这种趋势：

- **横向扩展** 构建管理N个集群的能力
- **纵向扩展** 提高单个集群的规模

要扩展单个集群的规模，元信息/状态信息的存储是核心的扩展点之一，本项目就是解决集群状态信息存储的可扩展性和性能问题。

我们调研了一部分现有的分布式存储系统，也分析了ETCD的性能瓶颈 和kubernetes对于状态信息存储的接口需求。期间受到[kine项目](https://github.com/k3s-io/kine) 的启发，实现KubeBrain，作为Kubernetes状态信息存储的核心服务。

# 项目特点

- **无状态**
  KubeBrain作为一个实现API Server所需要使用的存储服务端接口的组件进行存储接口的转换并不实际存储数据，实际的元数据存放在底层的存储引擎中，而API Server所需要监听的数据存放主节点内存中。
- **扩展性**
  KubeBrain抽象了键值数据库接口, 在此基础上实现存储API Server存储所需要使用的接口, 具有指定特性的键值数据库均可适配存储接口。
- **高可用**
  KubeBrain当前采用主从架构，主节点支持包括条件更新、读、事件监听在内所有操作，从节点支持读操作，基于K8S的[leaderelection](https://github.com/kubernetes/client-go/tree/master/tools/leaderelection)
  进行自动选主，实现高可用。
- **水平扩容**
  生产环境下，KubeBrain通常使用分布式键值数据库存储数据，水平扩容包含两个层面：
    - 在KubeBrain的层面，可以通过增加从节点提高并发读性能；
    - 在存储引擎层面，可以通过增加存储节点等手段提高读写性能和存储容量。

# 详细文档

- [快速开始](./docs/quick_start_cn.md)
- [架构设计](./docs/design_in_detail_cn.md)
- [存储引擎](./docs/storage_engine_cn.md)
- [性能测试](./docs/benchmark_cn.md)

# TODO

- [ ] 优化存储引擎接口
- [ ] 极端情况下的一致性保证
- [ ] 内置的逻辑时钟
- [ ] 优化单元测试代码，增加用例和错误注入
- [ ] [Jepsen测试](https://jepsen.io/)
- [ ] 实现Proxy功能

# 贡献代码

[Contributing](CONTRIBUTING.md)

# 开源许可

KubeBrain基于[Apache License 2.0](LICENSE)许可证。

# 联系我们

- Email: [xuchen.xiaoying@bytedance.com](xuchen.xiaoying@bytedance.com)