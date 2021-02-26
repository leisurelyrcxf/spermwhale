.PHONY: deps build binary

REPO_PATH := github.com/leisurelyrcxf/spermwhale
REVISION := $(shell git rev-parse HEAD || unknown)
BUILTAT := $(shell date +%Y-%m-%dT%H:%M:%S)
VERSION := $(shell git describe --tags $(shell git rev-list --tags --max-count=1))
GO_LDFLAGS ?= -s -X $(REPO_PATH)/versioninfo.REVISION=$(REVISION) \
			  -X $(REPO_PATH)/versioninfo.BUILTAT=$(BUILTAT) \
			  -X $(REPO_PATH)/versioninfo.VERSION=$(VERSION)
GO_MAJOR_VERSION = $(shell go version | cut -c 14- | cut -d' ' -f1 | cut -d'.' -f2)
MINIMUM_SUPPORTED_GO_MAJOR_VERSION = 13

deps:
	echo "GO_MAJOR_VERSION: $(GO_MAJOR_VERSION)"
	go env
	env GO111MODULE=on go mod download
	env GO111MODULE=on go mod vendor

binary:
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o sptablet ./cmd/tablet
	go build -ldflags "$(GO_LDFLAGS)" -a -tags "netgo osusergo" -installsuffix netgo -o spgate ./cmd/gate


build: deps binary

clean-build: clean build

clean:
	rm go.sum || true
	rm -r vendor/ || true

mock:
	cd controller/pika/ && mockery --name Controller
	cd store/ && mockery --name Storage

test: deps unit-test

cloc:
	cloc --exclude-dir=vendor,3rdmocks,mocks,tools --not-match-f=test .

unit-test:
	go vet `go list ./... | grep -v '/vendor/' | grep -v '/tools'`
	go test -timeout 120m -count=1 -cover ./...

fmt:
	go list ./... | grep -v '/vendor/' | grep -v '/tools/' | xargs -I {} -n 1 find "${GOPATH}/src/{}/" -maxdepth 1 -iname "*.go" | xargs -n 1 goreturns -w -l

cb: clean-build

lint:
	golangci-lint run || true

fml: fmt lint
