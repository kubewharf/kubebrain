package storage

func NonsupportTSO(storage KvStorage) bool {
	_, ok := storage.(interface {
		NonsupportTSO()
	})
	return ok
}
