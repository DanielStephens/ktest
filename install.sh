DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# update the existing pom with any change in deps.edn dependencies
clj -Spom
# set the new version
mvn versions:set -DnewVersion="$1" -DgenerateBackupPoms=false

# generate java classes
clj -M:build
# build a jar of the library
clj -Sdeps '{:deps {seancorfield/depstar {:mvn/version "1.1.128"}}}' -M -m hf.depstar.jar 'target/ktest.jar'

# install the jar to maven
clj -X:deps mvn-install :jar '"target/ktest.jar"' :pom '"pom.xml"'
