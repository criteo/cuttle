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
  name: 'mysqld',
  httpPort: 3388,
  sh: 'java -cp `cat /tmp/classpath_org.criteo.langoustine.localdb` org.criteo.langoustine.localdb.LocalDB',
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
    MYSQL_HOST: 'localhost',
    MYSQL_PORT: '3388',
    MYSQL_DATABASE: 'langoustine_dev',
    MYSQL_USER: 'root',
    MYSQL_PASSWORD: ''
  }
}).dependsOn(compile, generateClasspath, database);

proxy(server, 8080).dependsOn(front);
