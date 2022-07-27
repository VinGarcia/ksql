
GOBIN=$(shell go env GOPATH)/bin

TIME=1s
bench: gen
	go test -bench=. -benchtime=$(TIME)

lint: gen
	@$(GOBIN)/staticcheck ./...
	@go vet ./...

gen: sqlcfiles

sqlcfiles: $(GOBIN)/sqlc sqlc.yaml schema.sql sqlcgen/queries.sql
	sqlc generate

$(GOBIN)/sqlc:
	go install github.com/kyleconroy/sqlc/cmd/sqlc@latest

$(GOBIN)/sqlboiler:
	go install github.com/volatiletech/sqlboiler/v4@latest
	go install github.com/volatiletech/sqlboiler/v4/drivers/sqlboiler-psql@latest
