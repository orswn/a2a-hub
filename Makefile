# Local runs take Go and Node from mise.toml. CI sets RUN= and installs the tools itself.
RUN ?= mise exec --
GOLANGCI_LINT ?= mise exec aqua:golangci/golangci-lint@2.15.0 -- golangci-lint
HADOLINT ?= mise exec aqua:hadolint/hadolint@2.14.0 -- hadolint
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null | sed 's/^v//')

.PHONY: verify fmt-check vet lint go-test protocol-test contract build bin/a2a-hub vuln containerfile-lint clean

verify: fmt-check vet lint go-test protocol-test contract

fmt-check:
	@out="$$($(RUN) gofmt -l $$(git ls-files '*.go'))"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet:
	$(RUN) go vet ./...

lint:
	$(RUN) $(GOLANGCI_LINT) run

go-test:
	$(RUN) go test -race ./...

# Always rebuilt, so the contract suite runs against the current sources. The Go build cache keeps it fast.
bin/a2a-hub:
	CGO_ENABLED=0 $(RUN) go build -trimpath -ldflags "-s -w -X main.version=$(VERSION)" -o $@ ./cmd/a2a-hub

build: bin/a2a-hub

protocol/node_modules/.package-lock.json: protocol/package-lock.json
	$(RUN) npm --prefix protocol ci --no-audit --no-fund

test/node_modules/.package-lock.json: test/package-lock.json
	$(RUN) npm --prefix test ci --no-audit --no-fund

protocol-test: protocol/node_modules/.package-lock.json
	$(RUN) npm --prefix protocol test

contract: bin/a2a-hub test/node_modules/.package-lock.json
	HUB_BIN=$(CURDIR)/bin/a2a-hub $(RUN) npm --prefix test run contract

vuln:
	$(RUN) go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...

containerfile-lint:
	$(HADOLINT) Containerfile

clean:
	rm -rf bin
