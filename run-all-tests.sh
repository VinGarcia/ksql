#!/usr/bin/env bash

# Generate the coverate.txt file for all modules:
find . -name go.mod -execdir go test -coverprofile=coverage.txt -covermode=atomic -coverpkg=github.com/vingarcia/ksql ./... \;

# codecov will find all `coverate.txt` files, so it will work fine.
