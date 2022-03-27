#!/usr/bin/env bash

# Update go.mod with replace so testing will
# run against the local version of ksql:
echo "replace github.com/vingarcia/ksql => ../../" >> go.mod
go mod tidy
go test

# Save whether the tests succeeded or not:
tests_succedeed=$?

# Undo the changes:
git checkout go.mod go.sum > /dev/null

# Return error if the tests failed:
test $tests_succedeed -eq 0
