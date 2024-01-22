
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

# This recipe requires the ksql database to be setup
# exactly as described in the `sqlboiler.toml` file, that's
# why it is not running automatically before each benchmark.
sqlboilerfiles: $(GOBIN)/sqlboiler
	sqlboiler psql -c sqlboiler.toml --wipe --no-tests

$(GOBIN)/sqlc:
	go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest

$(GOBIN)/sqlboiler:
	go install github.com/volatiletech/sqlboiler/v4@latest
	go install github.com/volatiletech/sqlboiler/v4/drivers/sqlboiler-psql@latest
