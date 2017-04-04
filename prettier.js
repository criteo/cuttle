const format = require("prettier-eslint");
const args = require("yargs");
const path = require("path");
var fs = require("fs");

const file = args.argv.file;

if (file && file.split(path.sep).pop().match(/jsx?/)) {
  console.log("file prettyfied " + file);
  const fullpath = path.resolve(__dirname, file);
  const options = {
    filePath: fullpath,
    prettierOptions: {
      bracketSpacing: true
    }
  };

  const outputPrettyfied = format(options);
  console.log(outputPrettyfied);

  fs.writeFile(fullpath, outputPrettyfied, function(err) {
    console.log(err);
  });

} else {
  console.log("no file specified");
  console.log(args);
}
