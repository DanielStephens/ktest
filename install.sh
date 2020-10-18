DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# update the existing pom with any change in deps.edn dependencies
clj -Spom
# build an uberjar of the library
clojure -Sdeps '{:deps {uberdeps/uberdeps {:mvn/version "1.0.2"}}}' -m uberdeps.uberjar
# install the uberjar to maven
clojure -X:deps mvn-install :jar '"target/ktest.jar"' :pom '"pom.xml"'