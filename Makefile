.PHONY: lint test

lint:
	@echo "Running linter..."
	golangci-lint run --timeout 5m -c .golangci.yml

test:
	go test -v ./... -coverprofile=c,vxm  overage.out