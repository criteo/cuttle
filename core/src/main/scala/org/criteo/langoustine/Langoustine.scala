package org.criteo.langoustine

import lol.http._

import scala.concurrent.ExecutionContext.Implicits.global

object Langoustine {

    def run[S <: Scheduling](
      worflow: Graph[S],
      executionFrameworks: Seq[ExecutionFramework] = List(LocalFramework(maxTasks = 10)),
      httpPort: Int = 8888)(implicit scheduler: Scheduler[S]) = {
      scheduler.run(worflow, Executor(executionFrameworks))
      Server.listen(
        port = httpPort,
        onError = { e =>
          e.printStackTrace()
          InternalServerError("LOL.")
        })(App.routes)
      println(s"Listening on http://localhost:$httpPort")
    }

}
