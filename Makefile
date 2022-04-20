args=
path=./...

GOBIN=$(shell go env GOPATH)/bin

TIME=1s

test: setup
	$(GOBIN)/richgo test $(path) $(args)
	@( cd adapters/kpgx ; $(GOBIN)/richgo test $(path) $(args) )
	@( cd adapters/kmysql ; $(GOBIN)/richgo test $(path) $(args) )
	@( cd adapters/ksqlserver ; $(GOBIN)/richgo test $(path) $(args) )
	@( cd adapters/ksqlite3 ; $(GOBIN)/richgo test $(path) $(args) )

bench:
	cd benchmarks && go test -bench=. -benchtime=$(TIME)
	@echo "Benchmark executed at: $$(date --iso)"
	@echo "Benchmark executed on commit: $$(git rev-parse HEAD)"

lint: setup
	@$(GOBIN)/staticcheck $(path) $(args)
	@go vet $(path) $(args)
	@make --no-print-directory -C benchmarks
	@echo "StaticCheck & Go Vet found no problems on your code!"

gen: mock
mock: setup
	$(GOBIN)/mockgen -package=exampleservice -source=contracts.go -destination=examples/example_service/mocks.go

setup: $(GOBIN)/richgo $(GOBIN)/staticcheck $(GOBIN)/mockgen

$(GOBIN)/richgo:
	go get github.com/kyoh86/richgo

$(GOBIN)/staticcheck:
	go install honnef.co/go/tools/cmd/staticcheck@latest

$(GOBIN)/mockgen:
	@# (Gomock is used on examples/example_service)
	go get github.com/golang/mock/gomock
	go get github.com/golang/mock/mockgen

# Running examples:
exampleservice: mock
	$(GOPATH)/bin/richgo test ./examples/example_service/...
