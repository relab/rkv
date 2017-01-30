PKGS		 := $(shell go list ./... | grep -ve "vendor")
CMD_PKGS := $(shell go list ./... | grep -ve "vendor" | grep "cmd")
LIB_PKGS := $(shell go list ./... | grep -ve "vendor" | grep -ve "cmd")

.PHONY: all
all: install test

.PHONY: autocomplete
autocomplete:
	go install $(LIB_PKGS)

.PHONY: restore
restore:
	gvt restore

.PHONY: protocgorums
protocgorums:
	go install github.com/relab/gorums/cmd/protoc-gen-gorums

.PHONY: proto
proto: protocgorums
	protoc -I ../../../:. --gorums_out=plugins=grpc+gorums:. gorumspb/gorums.proto

.PHONY: install
install:
	@for pkg in $(CMD_PKGS); do \
		! go install $$pkg; \
		echo $$pkg; \
	done

.PHONY: test
test:
	go test $(PKGS) -v

.PHONY: bench
bench:
	go test $(PKGS) -v -run ^none -bench .

.PHONY: check
check:
	@gometalinter --config metalinter.json ./...

.PHONY: clean
clean:
	go clean -i $(CMD_PKGS)
