#!/usr/bin/env bash

# Make sure the script will stop on error:
set -ueo pipefail

# Generate the coverate.txt file for all modules:

# Run ksql root module tests:
go test -coverprofile=coverage.txt -coverpkg=github.com/vingarcia/ksql ./...

# Run the benchmarks
( cd benchmarks ; go test -coverprofile=coverage.txt -coverpkg=github.com/vingarcia/ksql ./... )

# Run the tests for the examples module:
( cd examples ; go test -coverprofile=coverage.txt -coverpkg=github.com/vingarcia/ksql ./... )

# Make sure the run-with-replace.sh is on PATH:
export PATH=$PATH:$(pwd)/scripts

# Then for each adapter run the tests with the replace directive:
( cd adapters/kpgx ; run-with-replace.sh go test -coverprofile=coverage.txt -coverpkg=github.com/vingarcia/ksql ./... )
( cd adapters/kpgx5 ; run-with-replace.sh go test -coverprofile=coverage.txt -coverpkg=github.com/vingarcia/ksql ./... )
( cd adapters/ksqlite3 ; run-with-replace.sh go test -coverprofile=coverage.txt -coverpkg=github.com/vingarcia/ksql ./... )
( cd adapters/modernc-ksqlite ; run-with-replace.sh go test -coverprofile=coverage.txt -coverpkg=github.com/vingarcia/ksql ./... )
( cd adapters/kpostgres ; run-with-replace.sh go test -coverprofile=coverage.txt -coverpkg=github.com/vingarcia/ksql ./... )
( cd adapters/ksqlserver ; run-with-replace.sh go test -coverprofile=coverage.txt -coverpkg=github.com/vingarcia/ksql ./... )
( cd adapters/kmysql ; run-with-replace.sh go test -coverprofile=coverage.txt -coverpkg=github.com/vingarcia/ksql ./... )

# codecov will find all `coverate.txt` files, so it will work fine.
