#!/bin/bash
set -eou pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

./install.sh $1
git tag -a "$1" -m "Releasing version $1"
git push origin --tags

mvn deploy:deploy-file \
      -Dfile=target/ktest.jar \
      -DrepositoryId=clojars \
      -Durl=https://clojars.org/repo \
      -DpomFile=pom.xml
