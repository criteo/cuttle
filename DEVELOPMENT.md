# Developing Cuttle

![Build status](https://api.travis-ci.org/criteo/cuttle.svg?branch=master)

To work on the Scala API, run sbt using `sbt -DdevMode=true` (the devMode
option allows to skip webpack execution to speed up execution). You can
use `~compile` or `~test` on the root project or focus on a specific
project using `~timeseries/test`.

To run an example application, use `example HelloWorld` where HelloWorld
is the example class name.

To work on the web application:

- Install [**yarn**](https://yarnpkg.com/en/)
- Run, `yarn install` and `yarn start`.

### Local Database

To run the embedded MySQL on Linux you will need:
 - ncurses 5, if your distribution is already using ncurses 6 you can probably install a
compatibility package (e.g. ncurses5-compat-libs on Archlinux).
 - libaio1.so, if your distribution has not it installed by default (ex: Ubuntu):
   ```
   $> sudo apt install libaio1
   ```

### Javascript style

We use [prettier-eslint](https://github.com/prettier/prettier-eslint) to enforce style.

Several ways to use it:

1. We provide a `git-init-precommit-hook.sh` that **replaces you git precommit hook** 
   by a Scalafmt and prettier-eslint run on changed files.
   
2. Use `yarn format` to format the Javascript files. 
3. Use `ỳarm format-diff` to format only changed files.


### Scala Style

We use [Scalafmt](http://scalameta.org/scalafmt/) to enforce style. The minimal config is found in the
.scalafmt.conf file (you probably won't make any friends if you change
this).

Several ways to use it:

1. We provide a `git-init-precommit-hook.sh` that **replaces you git precommit hook** by a Scalafmt run on changed files.

__Note__:
  - It works only in UNIX like systems with bash installed.
  - [You need a Scalafmt installed in your PATH in CLI mode](http://scalameta.org/scalafmt/#CLI).

2. You can install the [IntelliJ
plugin](http://scalameta.org/scalafmt/#IntelliJ) and use the familiar
shift-ctrl-L shortcut.

3. You can also use the sbt plugin:
```
$> sbt "scalafmt -i"
```
See also:
```
$> sbt "scalafmt -help"
```
You might also want to read details of the [sbt
integration](http://scalameta.org/scalafmt/#sbt).
