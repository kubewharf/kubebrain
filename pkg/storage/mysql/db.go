package mysql

import (
	"context"
	"fmt"
	"math"

	"github.com/pkg/errors"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/kubewharf/kubebrain/pkg/backend/coder"
	"github.com/kubewharf/kubebrain/pkg/storage"
)

type Config struct {
	UserName string
	Password string
	URL      string
	DBName   string
	Debug    bool
}

func (c Config) getDataSourceName() string {
	return fmt.Sprintf("%s:%s@(%s)/%s?charset=utf8mb4", c.UserName, c.Password, c.URL, c.DBName)
}

type store struct {
	conf Config
	db   *gorm.DB
}

func NewKvStorage(conf Config) (storage.KvStorage, error) {
	db, err := gorm.Open(mysql.Open(conf.getDataSourceName()), &gorm.Config{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to open db")
	}
	s := &store{
		conf: conf,
		db:   db,
	}

	err = s.init()
	if err != nil {
		return nil, err
	}

	if conf.Debug {
		s.db = s.db.Debug()
	}

	return s, nil
}

func (s *store) init() error {

	// check if table `object` exist
	err := s.db.AutoMigrate(&KV{})
	if err != nil {
		return errors.Wrap(err, "failed to init schema")
	}
	return nil
}

func (s *store) getClient(ctx context.Context) *gorm.DB {
	return s.db.WithContext(ctx)
}

func (s *store) GetTimestampOracle(ctx context.Context) (timestamp uint64, err error) {
	return 0, nil
}

func (s *store) GetPartitions(ctx context.Context, start, end []byte) (partitions []storage.Partition, err error) {
	return []storage.Partition{
		{
			Start: start,
			End:   end,
		},
	}, nil
}

func (s *store) Get(ctx context.Context, key []byte) (val []byte, err error) {
	kv, _ := decode(key)
	err = s.getClient(ctx).Where("rev = ?", kv.Rev).First(kv).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, storage.ErrKeyNotFound
		}
		return nil, errors.Wrap(err, "failed to get from db")
	}
	return kv.Val, nil
}

func (s *store) Iter(ctx context.Context, start []byte, end []byte, timestamp uint64, limit uint64) (storage.Iter, error) {
	i := newIter(s.db, start, end, int(limit))
	err := i.init(ctx)
	if err != nil {
		return nil, err
	}
	return i, err
}

func (s *store) BeginBatchWrite() storage.BatchWrite {
	b := &batch{
		db: s.getClient(context.Background()),
	}
	return b
}

func (s *store) Del(ctx context.Context, key []byte) (err error) {
	kv, _ := decode(key)
	err = s.db.Where("rev = ?", kv.Rev).Delete(kv).Error
	if err != nil {
		return errors.Wrap(err, "failed to delete")
	}
	return nil
}

func (s *store) DelCurrent(ctx context.Context, iter storage.Iter) (err error) {
	kv, _ := decode(iter.Key())
	err = s.db.Where("val = ?", iter.Val()).Where("rev = ?", kv.Rev).Delete(kv).Error
	if err != nil {
		return errors.Wrap(err, "failed to delete")
	}
	return nil
}

func (s *store) SupportTTL() bool {
	return false
}

func (s *store) Close() error {
	return nil
}

type KV struct {
	UserKey []byte `gorm:"primaryKey;type:VARCHAR(512)"`
	Rev     uint64 `gorm:"primaryKey"`
	Val     []byte `gorm:"type:LONGBLOB"`
}

var (
	defaultCoder = coder.NewNormalCoder()
)

func decode(internalKey []byte) (kv *KV, err error) {
	kv = &KV{}
	kv.UserKey, kv.Rev, err = defaultCoder.Decode(internalKey)
	if err != nil {
		kv.UserKey = internalKey
		// ! user MaxUint64 to avoid internal key be compacted
		kv.Rev = math.MaxUint64
	}
	return kv, nil
}

func encode(kv *KV) []byte {
	return defaultCoder.EncodeObjectKey(kv.UserKey, kv.Rev)
}
