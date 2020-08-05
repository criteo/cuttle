package com.criteo.cuttle.cron

import com.criteo.cuttle.Tag
import io.circe._
import io.circe.syntax._

import scala.language.implicitConversions

//A non-cyclic DAG, convention is that child depends on parents
case class CronPipeline(vertices: Set[CronJob], edges: Set[Dependency]) {

  def children: Set[CronJob] = edges.map(_.to)

  def roots: Set[CronJob] = vertices.filter(!children.contains(_))

  private[cron] def parents: Set[CronJob] = edges.map(_.from)

  private[cron] def leaves: Set[CronJob] = vertices.filter(!parents.contains(_))

  private[cron] def parentsMap = edges.groupBy(_.to)
  private[cron] def childrenMap = edges.groupBy(_.from)

  def dependsOn(right: CronPipeline): CronPipeline = {
    val left = this
    val newEdges: Set[Dependency] = for {
      v1 <- left.roots
      v2 <- right.leaves
    } yield Dependency(from = v2, to = v1)
    val duplicates = left.vertices.map(_.id).intersect(right.vertices.map(_.id))
    if (duplicates.nonEmpty) {
      throw new Exception("Duplicate job ids: " + duplicates.mkString(","))
    }
    new CronPipeline(
      vertices = left.vertices ++ right.vertices,
      edges = left.edges ++ right.edges ++ newEdges
    )
  }

  def and(other: CronPipeline): CronPipeline = {
    val leftWorkflow = this
    val duplicates = leftWorkflow.vertices.map(_.id).intersect(other.vertices.map(_.id))
    if (duplicates.nonEmpty) {
      throw new Exception("Duplicate job ids: " + duplicates.mkString(","))
    }
    new CronPipeline(
      vertices = leftWorkflow.vertices ++ other.vertices,
      edges = leftWorkflow.edges ++ other.edges
    )
  }

  /**
    * @param cronExpression Cron expression to be parsed by https://github.com/alonsodomin/cron4s.
    *                       See the link above for more details.
    */
  def toCronDag(cronExpression: String,
                id: String,
                name: String = "",
                description: String = "",
                tags: Set[Tag] = Set.empty[Tag]) =
    CronDag(id, this, CronExpression(cronExpression), name, description, tags)
}

object CronPipeline {
  implicit def fromCronJob(job: CronJob): CronPipeline = new CronPipeline(Set(job), Set.empty)

  implicit val encodePipeline: Encoder[CronPipeline] = new Encoder[CronPipeline] {
    override def apply(pipeline: CronPipeline) =
      Json.obj(
        "vertices" -> Json.arr(pipeline.vertices.map(_.id.asJson).toSeq: _*),
        "edges" -> Json.arr(pipeline.edges.map(_.asJson).toSeq: _*)
      )
  }
}

// "to" depends on "from", or "from" -> "to"
case class Dependency(from: CronJob, to: CronJob)

object Dependency {
  implicit val encodeDependency: Encoder[Dependency] = new Encoder[Dependency] {
    override def apply(dependency: Dependency) =
      Json.obj {
        "from" -> dependency.from.id.asJson
        "to" -> dependency.to.id.asJson
      }
  }
}
