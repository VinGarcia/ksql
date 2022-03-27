#!/usr/bin/env bash

# Make sure the script will stop on error:
set -ueo pipefail

# Generate the coverate.txt file for all modules:

# Run ksql root module tests:
go test -coverprofile=coverage.txt -covermode=atomic -coverpkg=github.com/vingarcia/ksql ./...

# Run the tests for the examples module:
( cd examples ; go test -coverprofile=coverage.txt -covermode=atomic -coverpkg=github.com/vingarcia/ksql ./... )

# Make sure the run-with-replace.sh is on PATH:
export PATH=$PATH:$(pwd)/scripts

# Then for each adapter run the tests with the replace directive:
for dir in $(find adapters -name go.mod -printf '%h\n'); do
  ( cd $dir ; run-with-replace.sh go test -coverprofile=coverage.txt -covermode=atomic -coverpkg=github.com/vingarcia/ksql ./... )
done

# codecov will find all `coverate.txt` files, so it will work fine.
