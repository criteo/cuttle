package com.criteo.cuttle

import lol.http._
import scala.concurrent.ExecutionContext.Implicits.global

class Cuttle[S <: Scheduling](
  project: Project,
  workflow: Graph[S],
  scheduler: Scheduler[S],
  ordering: Ordering[S#Context],
  retryStrategy: RetryStrategy[S],
  queries: Queries
) {
  def run(
    platforms: Seq[ExecutionPlatform[S]] = List(LocalPlatform(maxTasks = 10)(ordering)),
    httpPort: Int = 8888,
    databaseConfig: DatabaseConfig = Database.configFromEnv
  ) = {
    val database = Database.connect(databaseConfig)
    val executor = Executor[S](platforms, queries, database)(retryStrategy, ordering)
    Server.listen(port = httpPort, onError = { e =>
      e.printStackTrace()
      InternalServerError(e.getMessage)
    })(App(project, workflow, scheduler, executor).routes)
    println(s"Listening on http://localhost:$httpPort")
    scheduler.run(workflow, executor, database)
  }
}

object Cuttle {
  def apply[S <: Scheduling](name: String, description: Option[String] = None)(workflow: Graph[S])(
    implicit scheduler: Scheduler[S],
    retryStrategy: RetryStrategy[S],
    ordering: Ordering[S#Context]): Cuttle[S] =
    new Cuttle(Project(name, description), workflow, scheduler, ordering, retryStrategy, new Queries {})
}

case class Project(name: String, description: Option[String] = None)
