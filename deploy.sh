DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

./install.sh $1

git tag -a "$1" -m "Deployed version $1" -f

mvn deploy:deploy-file \
      -Dfile=target/ktest.jar \
      -DrepositoryId=clojars \
      -Durl=https://clojars.org/repo \
      -DpomFile=pom.xml

echo "Depending on the state of your branch, you should run"
echo "git add -u && \\"
echo "git commit -m 'Deployed version $1' && \\"
echo "git push && \\"
echo "git push --tags && \\"
