let sbt = startSbt({
  sh: 'sbt -DdevMode=true',
  watch: ['build.sbt']
});

let compile = sbt.run({
  command: 'compile',
  watch: ['**/*.scala']
});

let generateClasspath = sbt.run({
  name: 'classpaths',
  command: 'writeClasspath'
});

let database = runServer({
  name: 'postgres',
  httpPort: 5488,
  sh: 'java -cp `cat /tmp/classpath_org.criteo.langoustine.localpostgres` org.criteo.langoustine.localpostgres.LocalPostgres',
}).dependsOn(generateClasspath)

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
  env: {
    POSTGRES_HOST: 'localhost',
    POSTGRES_PORT: '5488',
    POSTGRES_DATABASE: 'langoustine',
    POSTGRES_USER: 'root',
    POSTGRES_PASSWORD: 'secret'
  }
}).dependsOn(compile, generateClasspath, database);

proxy(server, 8080).dependsOn(front);
