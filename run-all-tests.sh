#!/usr/bin/env bash

# Generate the coverate.txt file for all modules:
find . -name go.mod -execdir go test -coverprofile=coverage.txt -covermode=atomic -coverpkg=github.com/vingarcia/ksql ./... \;

# Merge all coverage files:
echo 'mode: atomic' > coverage.txt
find . -name coverage.txt -exec cat partial-coverage.txt \; | grep -v 'mode: atomic' | sort >> coverage.txt
