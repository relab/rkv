.PHONY: all
all: install test

.PHONY: install
install:
	go install -v ./cmd/...

.PHONY: proto
proto:
	protoc -I ../../../:. --gogofast_out=. cmdpb/cmd.proto

.PHONY: test
test:
	go test ./cmd/... -v

.PHONY: bench
bench:
	go test ./cmd/... -v -run ^none -bench .

.PHONY: check
check:
	gometalinter --config metalinter.json ./...

.PHONY: clean
clean:
	go clean -i ./cmd/...
