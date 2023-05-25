#!/bin/bash
set -eou pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# update the existing pom with any change in deps.edn dependencies
clj -Spom

# generate java classes
clj -M:build
# run tests
clj -M:test
