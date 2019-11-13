package com.criteo.cuttle.cron
import com.criteo.cuttle.{Job, Tag}
import io.circe._
import io.circe.syntax._

import scala.language.implicitConversions

/**
  * @param id The internal job id. It will be sued to track the job state in the database, so it must not
  *           change over time otherwise the job will be seen as a new one by the scheduler.
  *           That id, being technical, should only use valid characters such as [a-zA-Z0-9_-.]
  * @param s
  * @param cronPipeline dag of CronJobParts
  *
  */
case class CronJob(
   val id: String,
   name: String,
   scheduling: CronScheduling,
   pipeline: CronPipeline[CronJobPart],
   description: String = "",
   tags: Set[Tag] = Set.empty[Tag])

object CronJob {
  /**
    * Backward compatibility for Cuttle Jobs to CronJobs.
    * @param job
    * @return
    */
  implicit def jobToCronJob(job: Job[CronScheduling]): CronJob = {
    CronJob(job.id, job.name, job.scheduling, new CronJobPart(id = job.id, job.scheduling.maxRetry, name = job.name, description = job.description, job.tags) (job.effect), job.description, job.tags)
  }

  /**
    * Backward compatibility for CronJob to Cuttle Job.
    * @param cronJob
    * @return
    */
  def cronJobToJob(cronJob: CronJob) : Job[CronScheduling] = {
    Job[CronScheduling](cronJob.id, cronJob.scheduling, cronJob.name, cronJob.description, cronJob.tags)(null)
  }

  implicit val encodeUser: Encoder[CronJob] = new Encoder[CronJob] {
    override def apply(cronJob: CronJob) =
      Json
        .obj(
          "id" -> cronJob.id.asJson,
          "name" -> Option(cronJob.name).filterNot(_.isEmpty).getOrElse(cronJob.id).asJson,
          "description" -> Option(cronJob.description).filterNot(_.isEmpty).asJson,
          "scheduling" -> cronJob.scheduling.asJson,
          "tags" -> cronJob.tags.map(_.name).asJson,
          "pipeline" -> cronJob.pipeline.asJson
        )
        .asJson
  }
}

