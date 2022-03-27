#!/usr/bin/env bash

cmd=$@

# Update go.mod with replace so testing will
# run against the local version of ksql:
echo "replace github.com/vingarcia/ksql => ../../" >> go.mod
go mod tidy

# Run the input command:
eval $cmd

# Save whether the tests succeeded or not:
tests_succedeed=$?

# Undo the changes:
git checkout go.mod go.sum > /dev/null 2>&1

# Return error if the tests failed:
test $tests_succedeed -eq 0
