# KubeBrain on TiKV Benchmark

## Test Env

### Hardware

Tests are carried out using physical machines of the following

| Hardware | Properties                               |
|----------|------------------------------------------|
| CPU      | Intel(R) Xeon(R) Gold 5118 CPU @ 2.30GHz |
| MEM      | 187 GB                                   |
| DISK     | 2TB SSD                                  |
| NET      | 10 Gbps                                  |

### Deployment

#### etcd

3-node "etcd" cluster:
![benchmark_etcd-deployment](./images/benchmark_etcd_deployment.png)

### KubeBrain on TiKV

Use 3-node "KubeBrain" cluster, "TiKV" uses "TiUP" to deploy "3 PD + 3 KV" cluster. "KubeBrain", "PD", "KV" are mixed
deployed.

![benchmark_kube_brain_on_tikv_deployment](./images/benchmark_kube_brain_on_tikv_deployment.png)

## Benchmark

In the test case, the Key's length is fixed at 70 Bytes, generated with a "fixed prefix + random suffix". The Value is a
randomly generated 512 Bytes, with a concurrent output load of 300 etcd clients. Each test case is executed 3 times.

### Insert

[Detailed Data](./data/benchmark_insert.csv)
![table](./images/benchmark_table_graph/benchmark_workload_insert_throughput.png)

### Insert & Read

[Detailed Data](./data/benchmark_rw.csv)
![table](./images/benchmark_table_graph/benchmark_workload_ir_throughput.png)

### Delete

[Detailed Data](./data/benchmark_delete.csv)
![table](./images/benchmark_table_graph/benchmark_workload_delete_throughput.png)

## Conclusion

"KubeBrain on TiKV" can outperform "etcd" in read and write performance, while deletion performance needs to be further
optimized. However, the actual load of "Kubernetes" storage has a low percentage of deletion operations, and the overall
performance can be greater than "etcd". Moreover, "etcd" will have some performance degradation with usage, while "TiKV"
can be horizontally scaled to achieve better performance.