GOLANGCI_VERSION ?= v1.54.2
GOVULNCHECK_VERSION ?= v1.0.1

ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

.PHONY: default
default: test

.PHONY: install-govulncheck
install-govulncheck:
	@GOBIN=$(ROOT_DIR)/bin go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)

.PHONY: install-golangci
install-golangci:
	@test -f ./bin/golangci-lint || curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- $(GOLANGCI_VERSION)

.PHONY: go-lint
go-lint: install-govulncheck install-golangci
	@./bin/govulncheck ./...
	@./bin/golangci-lint run

.PHONY: lint
lint: go-lint

.PHONY: test-short
test-short:
	go test -cover -coverprofile=.coverage.out -count=10 -short -race -timeout 10s ./...

.PHONY: test
test:
	go test -cover -coverprofile=.coverage.out -count=2 -race -timeout 300s ./...

.PHONY: coverage-show
coverage-show:
	go tool cover -func=.coverage.out

.PHONY: coverage-html
coverage-html:
	go tool cover -html=.coverage.out

.PHONY: bench
bench:
	go test -bench . -test.benchmem -test.count 2 -test.benchtime 2s
