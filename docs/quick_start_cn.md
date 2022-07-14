# 快速开始

## KubeBrain编译与启动

### 基于badger的单机存储

KubeBrain适配了[dgraph-io/badger](https://github.com/dgraph-io/badger) 实现了单机版本用于测试

```shell
make badger
./bin/kube-brain --key-prefix "/"
```

### 基于TiKV的高可用存储

KubeBrain适配了[TiKV](https://github.com/tikv/tikv) ，实现了基于分布式键值数据库的kubernetes元信息存储

#### TiKV部署
TiKV集群部署参考[TiUP](https://docs.pingcap.com/zh/tidb/stable/production-deployment-using-tiup) ，本地测试环境可以使用`tiup playground`
```shell
tiup playground --mode tikv-slim --kv 1 --pd 1
```


## 编译与启动

```shell
make tikv
./bin/kube-brain --key-prefix "/" --pd-addr=127.0.0.1:2379 --port=3379 --peer-port=3380 --compatible-with-etcd=true
```
多个KubeBrain共用一个tikv集群时，注意配置
- `key-prefix`参数和apiserver对应的apiserver的`etcd-prefix`参数保持一致
- `compatible-with-etcd`需要置为true，以开启从节点的支持txn和watch，对etcd功能的完全兼容

## APIServer
### 使用社区版的APIServer
社区版APIServer存储仅支持etcd3的API，目前KubeBrain兼容了社区版APIServer中所使用到的etcd3的API，但是这种方式并不能充分发挥KubeBrain的优势

```shell
./kube-apiserver  /
    # ...
    --storage-backend=etcd3 \
    --etcd-servers=http://127.0.0.1:3379 \
    # ...

```



### 使用定制的APIServer
[定制的APIServer](todo) 通过扩展APIServer的[storage](https://github.com/kubernetes/kubernetes/blob/release-1.18/staging/src/k8s.io/apiserver/pkg/storage/interfaces.go) 接口，并在SDK中集成集群状态检查、读写分离负载等功能访问存储系统。定制版api-server会在之后开源。





