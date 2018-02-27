package com.criteo.cuttle

import scala.collection.mutable
import scala.concurrent.Future

import cats.Eq

/**
  * The workflow to be run by cuttle. A workflow is defined for a given [[Scheduling]],
  * for example it can be a [[timeseries.TimeSeries TimeSeries]] workflow.
  *
  * @tparam S The kind of [[Scheduling]] used by this workflow.
  **/
trait Workflow[S <: Scheduling] {
  private[criteo] type Dependency = (Job[S], Job[S], S#DependencyDescriptor)

  private[criteo] def vertices: Set[Job[S]]
  private[criteo] def edges: Set[Dependency]

  def roots: Set[Job[S]] = {
    val childNodes = edges.map { case (child, _, _) => child }
    vertices.filter(!childNodes.contains(_))
  }

  def leaves: Set[Job[S]] = {
    val parentNodes = edges.map { case (_, parent, _) => parent }
    vertices.filter(!parentNodes.contains(_))
  }

  // Returns a list of jobs in the workflow sorted topologically, using Kahn's algorithm. At the
  // same time checks that there is no cycle.
  private[cuttle] lazy val jobsInOrder: List[Job[S]] = topologicalSort((_, _) => ()) match {
    case Some(sortedJobs) => sortedJobs
    case None => throw new IllegalArgumentException("Workflow has at least one cycle")
  }

  /**
    * @return the list of strongly connected components (SCCs) in the workflow with more than 1 node, which
    * are the cycles in the graph
    * @note Tarjan algorithm
    */
  def findCycles(): List[List[Job[S]]] = {
    case class NodeWithMetadata(node: Job[S], children: mutable.Set[NodeWithMetadata] = mutable.Set.empty) {
      var discoveryTime: Long = Long.MaxValue
      // Discovery time of the earliest discovered ancestor
      var earliestAncestor: Long = Long.MaxValue

      def addChild(child: NodeWithMetadata): Boolean = children.add(child)

      def removeChild(child: NodeWithMetadata): Boolean = children.remove(child)

      def visited: Boolean = discoveryTime < Long.MaxValue

      // Override hash computation to prevent stack overflow when trying to compute the hash of a node using the
      // default hash method on workflows with cycles
      override def hashCode(): Int = node.hashCode()
    }

    // Perform DFS on each non visited node and pop up elements each time a strongly connected component is identified
    val stack = new mutable.Stack[NodeWithMetadata]
    // Bookkeeping of nodes whose SCC has not yet been identified
    val nodesInStack = mutable.Set.empty[NodeWithMetadata]
    var time = 0L
    val stronglyConnectedComponents = mutable.ListBuffer.empty[List[Job[S]]]

    def dfs(node: NodeWithMetadata): Unit = {
      stack.push(node)
      nodesInStack += node
      time += 1
      node.discoveryTime = time
      node.earliestAncestor = time
      node.children.foreach { child =>
        if (!child.visited) {
          // (node -> child) is a tree edge
          dfs(child)
          node.earliestAncestor = Math.min(node.earliestAncestor, child.earliestAncestor)
        }
        else if (nodesInStack.contains(child)) {
            // (node -> child) is a back edge
            node.earliestAncestor = Math.min(node.earliestAncestor, child.discoveryTime)
        }
        // Otherwise 'child' was already added to a SCC
      }

      // Depth-first search tree of 'node' full explored
      if (node.discoveryTime == node.earliestAncestor) {
        // 'node' is the head node of a SCC. The nodes in the 'stack' above it are part of the same SCC
        val scc = mutable.ListBuffer.empty[Job[S]]
        while (stack.top != node) {
          scc += stack.top.node
          nodesInStack -= stack.top
          stack.pop()
        }
        scc += stack.top.node
        nodesInStack -= stack.top
        stack.pop()
        stronglyConnectedComponents += scc.toList
      }
    }

    val nodesWithMetadata = vertices.map(job => NodeWithMetadata(job))
      .map(node => node.node -> node).toMap
    edges.foreach { case(child, parent, _) => nodesWithMetadata(parent).addChild(nodesWithMetadata(child)) }

    nodesWithMetadata.values.foreach { node =>
      if (!node.visited) dfs(node)
    }

    stronglyConnectedComponents.filter(_.size >= 2).toList
  }

  /**
    * @param edgeVisitor method called for each edge (parent node, child node) visited by Kahn's algorithm
    * @return the jobs in the workflow sorted in topological order, None if the graph contains a cycle
    * */
  def topologicalSort(edgeVisitor: (Job[S], Job[S]) => Unit = (_, _) => ()): Option[List[Job[S]]] = {
    val topologicalOrder = mutable.ListBuffer.empty[Job[S]]

    case class NodeWithMetadata(node: Job[S], children: mutable.Set[NodeWithMetadata] = mutable.Set.empty) {
      private var numIncomingEdges: Long = 0

      def addChild(child: NodeWithMetadata): Boolean = {
        child.numIncomingEdges += 1
        children.add(child)
      }
      def removeChild(child: NodeWithMetadata): Boolean = {
        child.numIncomingEdges -= 1
        children.remove(child)
      }
      /** @return true if the node has no incoming edges */
      def isOrphanNode: Boolean = numIncomingEdges == 0

      // Override hash computation to prevent stack overflow when trying to compute the hash of a node using the
      // default hash method on workflows with cycles
      override def hashCode(): Int = node.hashCode()
    }

    val nodesWithMetadata = vertices.map(job => NodeWithMetadata(job))
      .map(node => node.node -> node).toMap
    edges.foreach { case(child, parent, _) => nodesWithMetadata(parent).addChild(nodesWithMetadata(child)) }

    val rootsToVisit = collection.mutable.Set(roots.map(nodesWithMetadata(_)).toSeq:_*)

    while (rootsToVisit.nonEmpty) {
      val root = rootsToVisit.head
      topologicalOrder.append(root.node)
      rootsToVisit.remove(root)

      root.children.foreach { child =>
        edgeVisitor(root.node, child.node)
        root.removeChild(child)
        if (child.isOrphanNode) rootsToVisit.add(child)
      }
    }

    val hasCycle = !nodesWithMetadata.values.forall(node => node.isOrphanNode)
    if(hasCycle) None else Some(topologicalOrder.toList)
  }

  /**
    * Compose a [[Workflow]] with another [[Workflow]] but without any
    * dependency. It won't add any edge to the graph.
    *
    * @param otherWorflow The workflow to compose this workflow with.
    */
  def and(otherWorflow: Workflow[S]): Workflow[S] = {
    val leftWorkflow = this
    new Workflow[S] {
      val vertices = leftWorkflow.vertices ++ otherWorflow.vertices
      val edges = leftWorkflow.edges ++ otherWorflow.edges
    }
  }

  /**
    * Compose a [[Workflow]] with a second [[Workflow]] with a dependencies added between
    * all this workflow roots and the other workflow leaves. The added dependencies will use the
    * default dependency descriptors implicitly provided by the [[Scheduling]] used by this workflow.
    *
    * @param rightWorkflow The workflow to compose this workflow with.
    * @param dependencyDescriptor If injected implicitly, default dependency descriptor for the current [[Scheduling]].
    */
  def dependsOn(rightWorkflow: Workflow[S])(implicit dependencyDescriptor: S#DependencyDescriptor): Workflow[S] =
    dependsOn((rightWorkflow, dependencyDescriptor))

  /**
    * Compose a [[Workflow]] with a second [[Workflow]] with a dependencies added between
    * all this workflow roots and the other workflow leaves. The added dependencies will use the
    * specified dependency descriptors.
    *
    * @param rightWorkflow The workflow to compose this workflow with.
    * @param dependencyDescriptor The dependency descriptor to use for the newly created dependency edges.
    */
  def dependsOn(rightOperand: (Workflow[S], S#DependencyDescriptor)): Workflow[S] = {
    val (rightWorkflow, depDescriptor) = rightOperand
    val leftWorkflow = this
    val newEdges: Set[Dependency] = for {
      v1 <- leftWorkflow.roots
      v2 <- rightWorkflow.leaves
    } yield (v1, v2, depDescriptor)
    new Workflow[S] {
      val vertices = leftWorkflow.vertices ++ rightWorkflow.vertices
      val edges = leftWorkflow.edges ++ rightWorkflow.edges ++ newEdges
    }
  }

  case class DependencyBuilder(workflow: Workflow[S], parentJob: Job[S])(implicit dependencyDescriptor: S#DependencyDescriptor) {
    def to(childJob: Job[S]): Workflow[S] = {
      new Workflow[S] {
        val vertices = workflow.vertices ++ Set(childJob, parentJob)
        val edges = workflow.edges + ((childJob, parentJob, dependencyDescriptor))
      }
    }
  }

  def withDependencyFrom(parentJob: Job[S])(implicit dependencyDescriptor: S#DependencyDescriptor): DependencyBuilder =
    DependencyBuilder(this, parentJob)
}

/** Utilities for [[Workflow]]. */
object Workflow {

  /** An empty [[Workflow]] (empty graph). */
  def empty[S <: Scheduling]: Workflow[S] = new Workflow[S] {
    def vertices = Set.empty
    def edges = Set.empty
  }
}

/** Allow to tag a job. Tags can be used in the UI/API to filter jobs
  * and more easily retrieve them.
  *
  * @param name Tag name as displayed in the UI.
  * @param description Description as displayed in the UI.
  */
case class Tag(name: String, description: String = "")

/** The job [[SideEffect]] is the most important part as it represents the real
  * job logic to execute. A job is defined for a given [[Scheduling]],
  * for example it can be a [[timeseries.TimeSeries TimeSeries]] job. Jobs are also [[Workflow]] with a
  * single vertice.
  *
  * @tparam S The kind of [[Scheduling]] used by this job.
  * @param id the internal job id. It will be sued to track the job state in the database, so it must not
  *           change over time otherwise the job will be seen as a new one by the scheduler.
  * @param scheduling The scheduling configuration for the job. For example a [[timeseries.TimeSeries TimeSeries]] job can
  *                   be configured to be hourly or daily, etc.
  * @param name The job name as displayed in the UI.
  * @param description The job description as displayed in the UI.
  * @param tags The job tags used to filter jobs in the UI.
  * @param effect The job side effect, representing the real job execution.
  */
case class Job[S <: Scheduling](id: String,
                                scheduling: S,
                                name: String = "",
                                description: String = "",
                                tags: Set[Tag] = Set.empty[Tag])(val effect: SideEffect[S])
    extends Workflow[S] {
  private[criteo] val vertices = Set(this)
  private[criteo] val edges = Set.empty[Dependency]

  /** Run this job for the given [[Execution]].
    *
    * @param execution The execution instance.
    * @return A future indicating the execution result (Failed future means failed execution).
    */
  private[cuttle] def run(execution: Execution[S]): Future[Completed] = effect(execution)
}

case object Job {
  implicit def eqInstance[S <: Scheduling] = Eq.fromUniversalEquals[Job[S]]
}