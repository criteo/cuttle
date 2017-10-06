git checkout guillaume/doc
sbt "++2.12.3" unidoc -DdevMode=true
sbt "++2.12.3" examples/compile -DgenerateExamples -DdevMode=true
git checkout gh-pages
mkdir doc
rm -rf api
cp -rf target/scala-2.12/unidoc api
cp -rf examples/target/html examples0
rm -rf examples
cp -rf examples0 examples
rm -rf examples0
git add api examples
git commit -m "Update documentation"
git push origin gh-pages
