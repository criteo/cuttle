package com.criteo.cuttle.timeseries.graph.mutable

import scala.collection.mutable

private[cuttle] case class Node[T](data: T, children: mutable.Set[Node[T]] = mutable.Set.empty[Node[T]]) {
  private var inDegree: Long = 0

  def addChild(child: Node[T]): Boolean = {
    val childAdded = children.add(child)
    if (childAdded) child.inDegree += 1
    childAdded
  }

  def removeChild(child: Node[T]): Boolean = {
    val childRemoved = children.remove(child)
    if (childRemoved) child.inDegree -= 1
    childRemoved
  }

  /** @return true if the node has no incoming edges */
  def isOrphanNode: Boolean = inDegree == 0

  // Override hash computation to prevent stack overflow when trying to compute the hash of a node using the
  // default hash method on graphs with cycles
  override def hashCode(): Int = data.hashCode()
}

/** Represents a graph with an adjacency list */
private[cuttle] case class Graph[T](nodes: Seq[Node[T]])

private[cuttle] object Graph {

  /** @param edges set of edges defined as a pair (parent, child) */
  def fromVertexAndEdgeList[T](vertices: Set[T], edges: Set[(T, T)]): Graph[T] = {
    val verticesWithMetadata = vertices.map(data => data -> Node(data)).toMap
    edges.foreach { case (parent, child) => verticesWithMetadata(parent).addChild(verticesWithMetadata(child)) }
    Graph(verticesWithMetadata.values.toSeq)
  }
}
