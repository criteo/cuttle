# Langoustine, the good parts

To work on th Scala API, run sbt using `sbt -DdevMode=true` (the devMode
option allows to skip webpack execution to speed up execution). You can
use `~compile` or `~test` on the root project or focus on a specific
project using `~timeseries/test`.

To run an example application, use `example HelloWorld` where HelloWorld
is the example class name.

To work on the web application:

- Install **yarn** (https://yarnpkg.com/en/).
- Create a `dev.config.json` file in your project root:
  ```
    {
      "env": {
      },
      "example": "HelloWorld"
    }
  ```
- Run, `yarn install` and `yarn start`.


