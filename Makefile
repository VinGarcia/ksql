args=
path=./...

GOBIN=$(shell go env GOPATH)/bin

TIME=5s

test: setup go-mod-tidy
	$(GOBIN)/richgo test $(path) $(args)
	@( cd benchmarks ; $(GOBIN)/richgo test $(path) $(args) )
	@( cd examples ; $(GOBIN)/richgo test $(path) $(args) )
	@( cd adapters/kpgx ; $(GOBIN)/richgo test $(path) $(args) -timeout=20s )
	@( cd adapters/kpgx5 ; $(GOBIN)/richgo test $(path) $(args) -timeout=20s )
	@( cd adapters/kmysql ; $(GOBIN)/richgo test $(path) $(args) -timeout=20s )
	@( cd adapters/ksqlserver ; $(GOBIN)/richgo test $(path) $(args) -timeout=20s )
	@( cd adapters/ksqlite3 ; $(GOBIN)/richgo test $(path) $(args) -timeout=20s )
	@( cd adapters/modernc-ksqlite ; $(GOBIN)/richgo test $(path) $(args) -timeout=20s )

bench: go-mod-tidy
	@make --no-print-directory -C benchmarks TIME=$(TIME) | tee benchmark.tmp
	@echo "Benchmark executed at: $$(date --iso)" | tee -a benchmark.tmp
	@echo "Benchmark executed on commit: $$(git rev-parse HEAD)" | tee -a benchmark.tmp

readme: benchmark.tmp readme.template.md
	go run scripts/build-readme-from-template.go readme.template.md examples/crud/crud.go benchmark.tmp

lint: setup go-mod-tidy
	@$(GOBIN)/staticcheck $(path) $(args)
	@go vet $(path) $(args)
	@make --no-print-directory -C benchmarks lint
	@echo "StaticCheck & Go Vet found no problems on your code!"

# Run go mod tidy for all submodules:
go-mod-tidy:
	find . -name go.mod -execdir go mod tidy \;

# Create new tag and update adapters to use a new ksql tag:
version=v1.9.1
update:
	git tag $(version)
	git push origin master $(version)
	find adapters -name go.mod -execdir go get github.com/vingarcia/ksql@$(version) \;
	make go-mod-tidy
	git commit -am 'Update adapters to use version $(version)'
	git push origin master
	for dir in $$(ls adapters); do git tag adapters/$$dir/$(version); done
	for dir in $$(ls adapters); do git push origin adapters/$$dir/$(version); done

gen: mock
mock: setup
	$(GOBIN)/mockgen -package=exampleservice -source=contracts.go -destination=examples/example_service/mocks.go

setup: $(GOBIN)/richgo $(GOBIN)/staticcheck $(GOBIN)/mockgen go.work

go.work:
	go work init
	go work use -r .

$(GOBIN)/richgo:
	go install github.com/kyoh86/richgo@latest

$(GOBIN)/staticcheck:
	go install honnef.co/go/tools/cmd/staticcheck@latest

$(GOBIN)/mockgen:
	@# (Gomock is used on examples/example_service)
	go install github.com/golang/mock/mockgen@latest

# Running examples:
exampleservice: mock
	$(GOBIN)/richgo test ./examples/example_service/.

examplelogger: mock
	go run ./examples/logging_queries/.

PG_URL=
pgxsupport:
	go run ./examples/pgxsupport/.
