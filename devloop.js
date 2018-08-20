let sbt = startSbt({
  sh: "sbt -DdevMode=true",
  watch: ["build.sbt"]
});

let compile = sbt.run({
  command: "compile",
  watch: ["**/*.scala"]
});

let generateClasspath = sbt.run({
  name: "classpaths",
  command: "writeClasspath"
});

let database = runServer({
  name: "mysqld",
  httpPort: 3388,
  sh: "java -cp `cat /tmp/classpath_com.criteo.cuttle.localdb` com.criteo.cuttle.localdb.LocalDB"
}).dependsOn(generateClasspath);

let yarn = run({
  sh: "yarn install",
  watch: "package.json"
});

let front = webpack({
  watch: [
    "webpack.config.js",
    "core/src/main/javascript/**/*.*",
    "core/src/main/html/**/*.*",
    "core/src/main/style/**/*.*"
  ]
}).dependsOn(yarn, compile);

let server = runServer({
  httpPort: 8888,
  sh: "java -cp `cat /tmp/classpath_com.criteo.cuttle.examples` com.criteo.cuttle.examples.HelloTimeSeries",
  env: {
    MYSQL_LOCATIONS: "localhost:3388",
    MYSQL_DATABASE: "cuttle_dev",
    MYSQL_USERNAME: "root",
    MYSQL_PASSWORD: ""
  }
}).dependsOn(compile, generateClasspath, database);

proxy(server, 8080).dependsOn(front);
