package mysql

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"

	"github.com/kubewharf/kubebrain/pkg/storage"
)

func TestStore(t *testing.T) {
	ast := assert.New(t)
	st, err := NewKvStorage(Config{
		UserName: "root",
		Password: "",
		URL:      "127.0.0.1:4000",
		DBName:   "test",
		Debug:    true,
	})

	ast.NoError(err)
	ast.NotNil(st)

	ctx := context.Background()

	s := []byte("test")
	b := st.BeginBatchWrite()
	b.Put(s, s, 0)
	err = b.Commit(ctx)
	ast.NoError(err)

	val, err := st.Get(ctx, s)
	ast.Equal(s, val)
	ast.NoError(err)

	err = st.Del(ctx, s)
	ast.NoError(err)

	val, err = st.Get(ctx, s)
	ast.Equal(0, len(val))
	ast.True(errors.Is(err, storage.ErrKeyNotFound))
}
