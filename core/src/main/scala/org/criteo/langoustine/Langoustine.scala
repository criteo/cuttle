package org.criteo.langoustine

import lol.http._
import scala.concurrent.ExecutionContext.Implicits.global

class Langoustine[S <: Scheduling](
  project: Project,
  workflow: Graph[S],
  scheduler: Scheduler[S],
  ordering: Ordering[S#Context],
  queries: Queries
) {
  def run(
    platforms: Seq[ExecutionPlatform[S]] = List(LocalPlatform(maxTasks = 10)(ordering)),
    httpPort: Int = 8888,
    databaseConfig: DatabaseConfig = Database.configFromEnv
  ) = {
    val database = Database.connect(databaseConfig)
    val executor = Executor[S](platforms, queries, database)
    Server.listen(port = httpPort, onError = { e =>
      e.printStackTrace()
      InternalServerError("LOL")
    })(App(project, workflow, scheduler, executor).routes)
    println(s"Listening on http://localhost:$httpPort")
    scheduler.run(workflow, executor, database)
  }
}

object Langoustine {
  def apply[S <: Scheduling](name: String, description: Option[String] = None)(
    workflow: Graph[S])(implicit scheduler: Scheduler[S], ordering: Ordering[S#Context]): Langoustine[S] =
    new Langoustine(Project(name, description), workflow, scheduler, ordering, new Queries {})
}

case class Project(name: String, description: Option[String] = None)
