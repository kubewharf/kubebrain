
.PHONY: badger
badger:
	@bash ./build/build-badger.sh


.PHONY: tikv
tikv:
	@bash ./build/build-tikv.sh

.PHONY: test-coverage
test-coverage:
	@go test -coverprofile=coverage.out -cover=true -coverpkg=./pkg/... ./...