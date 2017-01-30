PKGS		 := $(shell go list ./... | grep -ve "vendor")
CMD_PKGS := $(shell go list ./... | grep -ve "vendor" | grep "cmd")

.PHONY: all
all: install test

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
