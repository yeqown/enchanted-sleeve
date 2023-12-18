.PHONY: lint test

lint:
	@echo "Running linter..."
	golangci-lint run --timeout 5m -c .golangci.yml

test:
	go test -v ./... -coverprofile=coverprofile.out

# TODO: add race detect command

build-cli:
	go install ./cmd/esl-ctl