package com.criteo.langoustinepp

import lol.http._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext
import scala.concurrent.ExecutionContext.Implicits.global

trait Scheduler[S <: Scheduling] {
  def run(graph: Graph[S])(implicit executor: ExecutionContext): Unit
}

trait Scheduling {
  type Context
  type DependencyDescriptor
}


sealed trait Graph[S <: Scheduling] {
  type Dependency = (Job[S], Job[S], S#DependencyDescriptor)

  private[langoustinepp] def vertices: Set[Job[S]]
  private[langoustinepp] def edges: Set[Dependency]

  def and(otherGraph: Graph[S]): Graph[S] = {
    val graph = this
    new Graph[S] {
      val vertices = otherGraph.vertices ++ graph.vertices
      val edges = otherGraph.edges ++ graph.edges
    }
  }

  private[langoustinepp] lazy val roots = vertices.filter (v =>
      edges.forall { case (v1, _, _) => v1 != v })
  private[langoustinepp] lazy val leaves = vertices.filter (v =>
      edges.forall { case (_, v2, _) => v2 != v })

  def dependsOn(right: Graph[S])
  (implicit depDescriptor: S#DependencyDescriptor): Graph[S] =
    dependsOn(right, depDescriptor)

  def dependsOn(right: (Graph[S], S#DependencyDescriptor)): Graph[S] = {
    val (rightGraph, depDescriptor) = right
    val leftGraph = this
    val newEdges: Set[Dependency] = for {
      v1 <- leftGraph.roots
      v2 <- rightGraph.leaves
    } yield (v1, v2, depDescriptor)
    new Graph[S] {
      val vertices = leftGraph.vertices ++ rightGraph.vertices
      val edges = leftGraph.edges ++ rightGraph.edges ++ newEdges
    }
  }
}

case class Job[S <: Scheduling](name: String, scheduling: S) extends Graph[S] {
  val vertices = Set(this)
  val edges = Set.empty[Dependency]
}

/* Ping-pong stuff */

object LangoustinePPServer {
  val routes: PartialService = {
    case GET at "/" =>
      Redirect("/ping")

    case GET at "/ping" =>
      Ok("pong!")
  }

  def main(args: Array[String]): Unit = {
    val httpPort = args.lift(0).map(_.toInt).getOrElse(8888)
    Server.listen(
      port = httpPort,
      onError = { e =>
        e.printStackTrace()
        InternalServerError("LOL.")
      })(routes)
    println(s"Listening on http://localhost:$httpPort")
  }
}
