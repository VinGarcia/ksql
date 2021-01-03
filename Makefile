args=
path=./...

GOPATH=$(shell go env GOPATH)

test: setup
	$(GOPATH)/bin/richgo test $(path) $(args)

lint: setup
	@$(GOPATH)/bin/golint -set_exit_status -min_confidence 0.9 $(path) $(args)
	@go vet $(path) $(args)
	@echo "Golint & Go Vet found no problems on your code!"

mock: setup
	mockgen -package=exampleservice -source=contracts.go -destination=examples/example_service/mocks.go

setup: .make.setup
.make.setup:
	go get github.com/kyoh86/richgo
	go get golang.org/x/lint
	@# (Gomock is used on examples/example_service)
	go get github.com/golang/mock/gomock
	go install github.com/golang/mock/mockgen
	touch .make.setup

# Running examples:
exampleservice: mock
	$(GOPATH)/bin/richgo test ./examples/example_service/...
