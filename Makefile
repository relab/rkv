.PHONY: all
all: install test

.PHONY: install
install:
	go install -v ./cmd/...

.PHONY: protoc
protoc:
	go get github.com/gogo/protobuf/protoc-gen-gogofaster

.PHONY: proto
proto: protoc
	protoc -I ../../../:. --gogofaster_out=plugins=grpc:. rkvpb/rkv.proto

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

.PHONY: prometheus
prometheus:
	prometheus -config.file=prometheus.yml -web.listen-address ":59090"
