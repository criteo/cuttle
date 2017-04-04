for file in $(find ./core/src/main/javascript/ -name "*.js"); do
  npm run prettify -- --file=$file;
done;
