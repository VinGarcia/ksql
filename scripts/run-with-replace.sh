#!/usr/bin/env bash

cmd=$@

# Default to ../../ for adapters at depth 2
# Can be overridden by setting KSQL_ROOT_PATH environment variable
root_path=${KSQL_ROOT_PATH:-../../}

# Update go.mod with replace so testing will
# run against the local version of ksql:
echo "replace github.com/vingarcia/ksql => $root_path" >> go.mod
go mod tidy

# Run the input command:
eval $cmd

# Save whether the tests succeeded or not:
tests_succedeed=$?

# Undo the changes:
git checkout go.mod go.sum > /dev/null 2>&1

# Return error if the tests failed:
test $tests_succedeed -eq 0
