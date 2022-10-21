module github.com/kubewharf/kubebrain

go 1.14

require (
	github.com/coreos/go-systemd v0.0.0-20190719114852-fd7a80b32e1f // indirect
	github.com/dgraph-io/badger v1.6.2
	github.com/evanphx/json-patch v0.5.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/mock v1.5.0
	github.com/google/btree v1.0.1 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/googleapis/gnostic v0.5.5 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.3.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0
	github.com/huandu/skiplist v1.1.0
	github.com/kr/text v0.2.0 // indirect
	github.com/kubewharf/kubebrain-client v0.2.1
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v1.12.1
	github.com/prometheus/client_model v0.2.0
	github.com/soheilhy/cmux v0.1.5
	github.com/spf13/cast v1.3.0
	github.com/spf13/cobra v1.1.3
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.8.0
	github.com/tikv/client-go/v2 v2.0.1
	go.etcd.io/etcd/api/v3 v3.5.2
	go.etcd.io/etcd/client/v3 v3.5.2
	golang.org/x/crypto v0.0.0-20221005025214-4161e89ecf1b // indirect
	golang.org/x/sync v0.0.0-20220722155255-886fb9371eb4
	golang.org/x/sys v0.0.0-20221006211917-84dc82d7e875 // indirect
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11 // indirect
	golang.org/x/tools v0.1.12 // indirect
	google.golang.org/grpc v1.43.0
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gorm.io/driver/mysql v1.4.3
	gorm.io/gorm v1.24.0
	k8s.io/api v0.20.4 // indirect
	k8s.io/apimachinery v0.20.4
	k8s.io/client-go v0.20.2
	k8s.io/component-base v0.20.2
	k8s.io/klog/v2 v2.4.0
	k8s.io/kube-openapi v0.0.0-00010101000000-000000000000 // indirect
)

replace (
	github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.3.1
	google.golang.org/grpc => google.golang.org/grpc v1.38.0
	k8s.io/api => k8s.io/api v0.0.0-20191004102349-159aefb8556b
	k8s.io/apiextensions-apiserver => k8s.io/apiextensions-apiserver v0.0.0-20191004105649-b14e3c49469a
	k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20191004074956-c5d2f014d689
	k8s.io/apiserver => k8s.io/apiserver v0.0.0-20191109015554-8577c320c87f
	k8s.io/cli-runtime => k8s.io/cli-runtime v0.0.0-20191004110135-b9eb767d2e1a
	k8s.io/client-go => k8s.io/client-go v11.0.1-0.20191029005444-8e4128053008+incompatible
	k8s.io/cloud-provider => k8s.io/cloud-provider v0.0.0-20191002184608-9779a9fba520
	k8s.io/csi-translation-lib => k8s.io/csi-translation-lib v0.0.0-20191016015547-9213b55ba309
	k8s.io/kube-openapi => k8s.io/kube-openapi v0.0.0-20190228160746-b3a7cee44a30
	k8s.io/kubernetes => k8s.io/kubernetes v1.14.8
	k8s.io/metrics => k8s.io/metrics v0.0.0-20191004105854-2e8cf7d0888c
	k8s.io/utils => k8s.io/utils v0.0.0-20200327001022-6496210b90e8
)
