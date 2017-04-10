if(!require('fs').existsSync('dev.config.json')) {
  throw new Error(`Missing dev.config.json file in your project, please check the README.`);
}
let config = loadJson('dev.config.json');

let sbt = startSbt({
  sh: 'sbt -DdevMode=true',
  watch: ['build.sbt']
});

let compile = sbt.run({
  command: 'compile',
  watch: ['**/*.scala']
});

let generateClasspath = sbt.run({
  command: 'examples/writeClasspath'
});

let yarn = run({
  sh: 'yarn install',
  watch: 'package.json'
});

let front = webpack({
  watch: [
    'webpack.config.js',
    'core/src/main/javascript/**/*.*',
    'core/src/main/html/**/*.*',
    'core/src/main/style/**/*.*'
  ]
}).dependsOn(yarn);

let server = runServer({
  httpPort: 8888,
  sh: 'java -cp `cat /tmp/classpath_org.criteo.langoustine.examples` org.criteo.langoustine.examples.HelloWorld',
  env: config.env || {}
}).dependsOn(compile, generateClasspath);

proxy(server, 8080).dependsOn(front);
