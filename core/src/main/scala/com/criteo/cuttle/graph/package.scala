package com.criteo.cuttle

import scala.collection.mutable.{Set => MutableSet}
import scala.collection.mutable.ListBuffer

import com.criteo.cuttle.graph.mutable.{Node => MutableNode}
import com.criteo.cuttle.graph.mutable.{Graph => MutableGraph}

package object graph {
  /**
    * @param edges set of edges defined as a pair (parent, child)
    * @return the nodes in `graph` sorted in topological order, None if the graph contains a cycle
    * @note Kahn's algorithm
    * */
  def topologicalSort[T](vertices: Set[T], edges: Set[(T, T)]): Option[List[T]] = {
    val topologicalOrder = ListBuffer.empty[MutableNode[T]]

    val graph = MutableGraph.fromVertexAndEdgeList(vertices, edges)

    val nodesToVisit = MutableSet(graph.nodes.filter(_.isOrphanNode): _*)

    while (nodesToVisit.nonEmpty) {
      val orphanNode = nodesToVisit.head
      topologicalOrder.append(orphanNode)
      nodesToVisit.remove(orphanNode)

      orphanNode.children.foreach { child =>
        // edgeVisitor(root.node, child.node)
        orphanNode.removeChild(child)
        if (child.isOrphanNode) nodesToVisit.add(child)
      }
    }

    val hasCycle = !graph.nodes.forall(node => node.isOrphanNode)
    if(hasCycle) None else Some(topologicalOrder.map(_.data).toList)
  }
}
