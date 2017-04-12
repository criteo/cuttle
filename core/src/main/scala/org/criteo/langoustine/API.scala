package org.criteo.langoustine

import io.circe._
import io.circe.generic.auto._
import io.circe.syntax._

object Api {
  implicit val projectEncoder = new Encoder[Project] {
    override def apply(project: Project): Json = {
      Map[String, Json](
        "name" -> project.name.asJson,
        "description" -> project.description.asJson
      ).asJson
    }
  }

  implicit def graphEncoder[S <: Scheduling] = {
    new Encoder[Graph[S]] {
      override def apply(workflow: Graph[S]): Json = {
        val jobs: Json = workflow.vertices.map { v =>
          Map[String, Json](
            "id" -> v.id.asJson,
            "name" -> v.name.asJson,
            "description" -> v.description.asJson,
            "tags" -> v.tags.map(_.name).asJson
          ).asJson
        }.asJson

        val tags = workflow.vertices.flatMap(_.tags).asJson

        val dependencies = workflow.edges.map { e =>
          Map[String, Json](
            "from" -> e._2.id.asJson,
            "to" -> e._1.id.asJson
          ).asJson
        }.asJson

        Map[String, Json](
          "jobs" -> jobs,
          "dependencies" -> dependencies,
          "tags" -> tags
        ).asJson
      }
    }
  }
}