# KubeBrain

English | [中文](./README_CN.md)

## Overview

Kubernetes is a distributed application orchestration and scheduling system. It has become the de facto standard for cloud-native application bases, but its official stable operation scale is limited to 5K nodes. This is sufficient for most application scenarios, but still insufficient for applications with millions of machine nodes. With the growth of "digitalisation" and "cloud-native" in particular, the overall global IT infrastructure will continue to grow at an accelerated rate. For distributed application orchestration and scheduling systems, there are two ways to adapt to this trend.

- Horizontal scaling: building the ability to manage N clusters.
- Vertical scaling: increasing the size of individual clusters.
  In order to scale a single cluster, the storage of meta/state information is one of the core scaling points, and this project is to solve the scalability and performance problems of cluster state information storage.

We investigated some of existing distributed storage systems, and analyzed the performance of ETCD and the interface usage in kubernetes for state information storage. Inspired by the kine project, KubeBrain was implemented as the core service for Kubernetes state information storage.

### Feature

- Stateless: KubeBrain is a component that implements the storage server interface required by the API Server. It performs the conversion of the storage interface and does not actually store the data. The actual metadata is stored in the underlying storage engine,  and the data that the API Server needs to monitor is stored in the memory of the master node.
- Extensibility: KubeBrain abstracts the key-value database interface, and implements the interface needed for storage API Server storage on this basis. All key-value databases with specified characteristics can be adapted to the storage interface.
- High availability: KubeBrain currently uses master-slave architecture. The master node supports all operations including conditional update, read, and event monitoring. The slave node supports read operations, and automatically selects the master based on K8S's "leaderelection" to achieve high availability.
- Horizontal scaling: In a production environment, KubeBrain usually uses a distributed key-value database to store data. Horizontal scaling involves two levels:
    - At the KubeBrain level, concurrent read performance can be improved by adding slave nodes;
    - At the storage engine level, read and write performance and storage capacity can be improved by adding storage nodes.

### Detailed Documentation
- [Quick Start](./docs/quick_start.md)
- [Architecture](./docs/design_in_detail.md)
- [Storage Engine Abstraction](./docs/storage_engine.md)
- [Benchmark](./docs/benchmark.md)

## TODO
- [ ] Guarantee consistence in critical cases
- [ ] Optimize storage engine interface
- [ ] Optimize unit test code, add use cases and error injection
- [ ] [Jepsen Test](https://jepsen.io/)
- [ ] Implement Proxy to make it more scalable


## Contribution

Please check [Contributing](CONTRIBUTING.md) for more details.

## Code of Conduct

Please check [Code of Conduct](CODE_OF_CONDUCT.md) for more details.

## Community

- Email: kubewharf.conduct@bytedance.com
- Member: Please refer to [Maintainers](./MAINTAINER.md).

## License

This project is licensed under the [Apache-2.0 License](LICENSE).


