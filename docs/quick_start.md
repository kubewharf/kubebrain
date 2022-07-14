# Quick Start

## Compilation and Startup of KubeBrain

### Badger-based Stand-alone Storage
KubeBrain adapts to dgraph-io/badger to implement a stand-alone version for testing.

```shell
make badger
./bin/kube-brain --key-prefix "/"
```



### TiKV-based High-availability Storage
KubeBrain adapts to TiKV to implement kubernetes meta-information storage based on distributed key-value database.

#### TiKV Deployment
For TiKV cluster deployment, please refer to TiUP. For the local test environment, you can use tiup playground.
```shell
tiup playground --mode tikv-slim --kv 1 --pd 1
```


#### Compilation and Startup
```shell
make tikv
./bin/kube-brain --key-prefix "/" --pd-addr=127.0.0.1:2379 --port=3379 --peer-port=3380 --compatible-with-etcd=true
```
make tikv
./bin/kube-brain --key-prefix "/" --pd-addr=127.0.0.1:2379 --port=3379 --peer-port=3380
When multiple KubeBrain share a tikv cluster
- `key-prefix` is consistent with the etcd-prefixparameter of the apiserver corresponding to the apiserver.
- `compatible-with-etcd` should set true to make all node 

## APIServer

### Use the Community Version of APIServer
The community version of the APIServer storage only supports the API of etcd3, and KubeBrain is compatible with the API of etc3 used in the community version of APIServer, but this method does not take full advantage of KubeBrain.
```shell
./kube-apiserver  /
  # ...
  --storage-backend=etcd3 \
  --etcd-servers=http://127.0.0.1:3379 \
  # ...
```

### Using the Custom APIServer
The customized APIServer accesses the storage system by extending the storage interface of APIServer, and integrating functions such as cluster state checking, read-write separation of load, etc. in the SDK. The custom api-server will be released soon.

