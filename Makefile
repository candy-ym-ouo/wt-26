BINARY := tsdb
DIST := dist
GOOS_VALUE := $(shell go env GOOS)
GOARCH_VALUE := $(shell go env GOARCH)
PACKAGE := $(DIST)/$(BINARY)-$(GOOS_VALUE)-$(GOARCH_VALUE).tar.gz

.PHONY: build test check package clean

build:
	mkdir -p $(DIST)
	go build -trimpath -o $(DIST)/$(BINARY) ./cmd/server

test:
	go test ./...

check:
	go test -race ./...
	go vet ./...
	test -z "$$(gofmt -l .)"

package: check build
	LC_ALL=C LANG=C tar -czf $(PACKAGE) -C $(DIST) $(BINARY) -C .. README.md
	@echo $(PACKAGE)

clean:
	rm -rf $(DIST)
