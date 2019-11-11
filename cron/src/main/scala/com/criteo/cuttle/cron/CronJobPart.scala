package com.criteo.cuttle.cron

import com.criteo.cuttle.{SideEffect, Tag}
import io.circe._
import io.circe.syntax._


/**
  * Like a Job, but without a separate scheduling.
  * It is part of a CronJob.
  * @param id The internal job id. It will be sued to track the job state in the database, so it must not
  *           change over time otherwise the job will be seen as a new one by the scheduler.
  *           That id, being technical, should only use valid characters such as [a-zA-Z0-9_-.]
  * @param name
  * @param description
  * @param tags
  * @param effect
  */
case class CronJobPart(id: String,
                   maxRetry: Int = 1,
                   name: String = "",
                   description: String = "",
                   tags: Set[Tag] = Set.empty[Tag])(val effect: SideEffect[CronScheduling]) extends CronPipeline[CronJobPart] {
  override val vertices = Set(this)
}

object CronJobPart {

  implicit val encoder = new Encoder[CronJobPart] {
    override def apply(c: CronJobPart) = {
      Json.obj(
        "id" -> c.id.asJson,
        "maxRetry" -> c.maxRetry.asJson,
        "name" -> c.name.asJson,
        "description" -> c.description.asJson,
        "tags" -> c.tags.asJson
      )
    }
  }

  implicit val dependencyEncoder = new Encoder[Dependency[CronJobPart]] {
    override def apply(d : Dependency[CronJobPart]) =
      Json.obj(
        "child" -> d.child.asJson,
        "parent" -> d.parent.asJson
      )
  }

  implicit val workflowEncoder =
    new Encoder[CronPipeline[CronJobPart]] {
      override def apply(p: CronPipeline[CronJobPart]) = {
        Json.obj(
          "jobs" -> p.vertices.map(_.asJson).asJson,
          "dependencies" -> p.edges.map(_.asJson).asJson
        )
      }
    }
}