for file in $(find ./src/main/javascript/ -name "*.js"); do
    npm run prettify -- --file=$file;
done;
