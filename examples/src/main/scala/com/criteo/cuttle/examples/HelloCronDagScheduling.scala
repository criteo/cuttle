package com.criteo.cuttle.examples
import com.criteo.cuttle.Completed
import com.criteo.cuttle.cron._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

object HelloCronDagScheduling {

  // A cuttle project is just embedded into any Scala application.
  def main(args: Array[String]): Unit = {
    val cronJobP1 = CronJobPart(id = "jobpart1", name = "job part 1", description = "this is job part 1") { implicit e =>
    Future {
      System.out.println(s"Job part 1")
      e.streams.info(s"Job part 1")
      Completed
    }}

    val cronJobP2 = CronJobPart(id = "jobpart2", name = "job part 2", description = "this is job part 2") { implicit e =>
      Future {
        System.out.println(s"Job part 2")
        e.streams.info(s"Job part 2")
        Completed
      }}

    val cronJobP3 = CronJobPart(id = "jobpart3", name = "job part 3", description = "this is job part 3") { implicit e =>
      Future {
        System.out.println(s"Job part 3")
        e.streams.info(s"Job part 3")
        Completed
      }}

    val workload = CronWorkload(
      Set(CronJob(id="cronjob", name="Cron Job",
        CronScheduling("0-59/10 * * ? * *", 1),
        cronJobP3 dependsOn (cronJobP1 and cronJobP2))))


    implicit val scheduler = CronScheduler(logger)

    // Project instantiation, it takes an implicit scheduler that we've just defined!
    val project = CronProject(
      name = "Hello Cron Scheduling Example",
      version = "0.0.1",
      description = "My first Cron with Cuttle project"
    )(workload)

    // __Finally start it!__
    project.start()
  }
}
