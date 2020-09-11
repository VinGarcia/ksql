args=
path=./...

GOPATH=$(shell go env GOPATH)

test: setup
	$(GOPATH)/bin/richgo test $(path) $(args)

lint: setup
	@$(GOPATH)/bin/golint -set_exit_status -min_confidence 0.9 $(path) $(args)
	@go vet $(path) $(args)
	@echo "Golint & Go Vet found no problems on your code!"

setup: .make.setup
.make.setup:
	go get github.com/kyoh86/richgo
	go get golang.org/x/lint
	touch .make.setup
