
GOBIN=$(shell go env GOPATH)/bin

TIME=1s
bench: sqlc
	go test -bench=. -benchtime=$(TIME)

lint: sqlc
	@$(GOBIN)/staticcheck ./...
	@go vet ./...

$(GOBIN)/sqlc:
	go install github.com/kyleconroy/sqlc/cmd/sqlc@latest

sqlc: $(GOBIN)/sqlc sqlcgen/schema.sql sqlcgen/queries.sql
	sqlc generate
