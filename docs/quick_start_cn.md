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


#### 编译与启动

```shell
make tikv
./bin/kube-brain --key-prefix "/" --pd-addr=127.0.0.1:2379 --port=3379 --peer-port=3380 --compatible-with-etcd=true
```
多个KubeBrain共用一个tikv集群时，注意配置
- `key-prefix`参数和apiserver对应的apiserver的`etcd-prefix`参数保持一致
- `compatible-with-etcd`需要置为true，以开启从节点的支持txn和watch，对etcd功能的完全兼容

### [实验性功能] 基于TiDB的高可用存储

KubeBrain适配了[TiDB](https://github.com/pingcap/tidb) ，实现了基于分布式键值数据库的kubernetes元信息存储

#### TiDB部署
TiDB集群部署参考[TiUP](https://docs.pingcap.com/zh/tidb/stable/production-deployment-using-tiup) ，本地测试环境可以使用`tiup playground`
```shell
tiup playground
```


#### 编译与启动

```shell
make mysql
./bin/kube-brain --key-prefix "/registry" --port=3379 --peer-port=3380 --db-name "mysql"
```

#### 使用 kind 拉起 kubernetes 测试集群

##### 安装 kind
```GO111MODULE="on" go get sigs.k8s.io/kind@v0.16.0```

##### 配置 kind 集群参数
替换 ip 为 kubebrain 的 ip
```
cat > cluster.yaml << EOF
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
- role: control-plane
- role: worker
kubeadmConfigPatches:
- |
  apiVersion: kubeadm.k8s.io/v1beta3
  kind: ClusterConfiguration
  apiServer:
    extraArgs:
      etcd-servers: "http://ip:3379"
      etcd-cafile: ""
      etcd-certfile: ""
      etcd-keyfile: ""
EOF
```

启动集群
```
 kind create cluster --name=brain --config=./cluster.yaml
```

```
cat > nginx.yaml << EOF
apiVersion: apps/v1 # for versions before 1.9.0 use apps/v1beta2
kind: Deployment
metadata:
  name: nginx
spec:
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: nginx
  replicas: 3 # tells deployment to run 1 pods matching the template
  template: # create pods using pod definition in this template
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: nginx
        ports:
        - containerPort: 80
EOF

kubectl create -f nginx.yaml

kubectl get pods

```




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





