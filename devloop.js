let config = loadJson('dev.config.json');

let sbt = startSbt({
  sh: 'sbt -DdevMode=true',
  watch: ['build.sbt']
});

let compile = sbt.run({
    command: 'compile',
    watch: ['src/**/*.scala']
});

let packageDependencies = sbt.run({
    command: 'assemblyPackageDependency'
});

let yarn = run({
  sh: 'yarn install',
  watch: 'package.json'
}).dependsOn(packageDependencies);

let front = webpack({
  watch: [
    'webpack.config.js',
    'src/main/javascript/**/*.js',
    'src/main/html/**/*.html',
    'src/main/style/**/*.*'
  ]
}).dependsOn(yarn);

let server = runServer({
  httpPort,
  sh: `java -cp target/scala-2.11/langoustinepp-assembly-0.1.0-deps.jar:target/scala-2.11/classes com.criteo.langoustinepp.LangoustinePPServer ${httpPort}`,
  env: config.env
}).dependsOn(compile, packageDependencies);

proxy(server, 8080).dependsOn(front);
