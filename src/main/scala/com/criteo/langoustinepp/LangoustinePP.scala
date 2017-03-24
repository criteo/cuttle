package com.criteo.langoustinepp

import lol.http._

import scala.language.implicitConversions
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

case class Langoustine[S <: Scheduling](graph: Graph[S]) {
  def run(): Unit = () /* FIXME */
}

trait Scheduling {
  type Context
  type DependencyDescriptor

  def defaultDependencyDescriptor: DependencyDescriptor
  implicit def graphToGraphAndDepDescriptor(job: Graph[this.type]): (Graph[this.type], DependencyDescriptor) =
    (job, defaultDependencyDescriptor)
}


trait Graph[S <: Scheduling] {
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

  private def roots = vertices.filter (v =>
      edges.forall { case (_, v2, _) => v2 != v })
  private def leaves = vertices.filter (v =>
      edges.forall { case (v1, _, _) => v1 != v })

  def dependsOn(right: (Graph[S], S#DependencyDescriptor)): Graph[S] = {
    val (rightGraph, depDescriptor) = right
    val leftGraph = this
    val newEdges: Set[Dependency] = for {
      v1 <- leftGraph.leaves
      v2 <- rightGraph.roots
    } yield (v1, v2, depDescriptor)
    new Graph[S] {
      val vertices = leftGraph.vertices ++ rightGraph.vertices
      val edges = leftGraph.edges ++ rightGraph.edges ++ newEdges
    }
  }
}

case class Job[S <: Scheduling](name: String) extends Graph[S] {
  val vertices = Set(this)
  val edges = Set.empty[Dependency]
}

/* Ping-pong stuff */

object LangoustinePP {

  val routes: PartialService = {
    case GET at "/" =>
      Redirect("/ping")

    case GET at "/ping" =>
      Ok("pong!")
  }
}

object LangoustinePPServer {

  def main(args: Array[String]): Unit = {
    val httpPort = args.lift(0).map(_.toInt).getOrElse(8888)
    Server.listen(
      port = httpPort,
      onError = { e =>
        e.printStackTrace()
        InternalServerError("LOL.")
      })(LangoustinePP.routes)
    println(s"Listening on http://localhost:$httpPort")
  }
}
