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

let packageDependencies = sbt.run({
    command: 'examples/assemblyPackageDependency'
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

let classpath = [
  'examples/target/scala-2.11/langoustine-assembly-dev-SNAPSHOT-deps.jar',
  'core/target/scala-2.11/classes',
  'timeserie/target/scala-2.11/classes',
  'examples/target/scala-2.11/classes'
];

let example = config.example || 'HelloWorld';

let server = runServer({
  httpPort,
  sh: `java -cp ${classpath.join(':')} org.criteo.langoustine.examples.${example} ${httpPort}`,
  env: config.env
}).dependsOn(compile, packageDependencies);

proxy(server, 8080).dependsOn(front);
