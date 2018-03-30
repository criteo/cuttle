package com.criteo.cuttle

import scala.collection.mutable.{ListBuffer, Stack, Map => MutableMap, Set => MutableSet}

import com.criteo.cuttle.graph.mutable.{Graph => MutableGraph, Node => MutableNode}

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

  /**
    * @param edges set of edges defined as a pair (parent, child)
    * @return the list of strongly connected components (SCCs) in `graph`
    * @note Tarjan algorithm
    */
  def findStronglyConnectedComponents[T](vertices: Set[T], edges: Set[(T, T)]): List[List[T]] = {
    val stronglyConnectedComponents = ListBuffer.empty[List[T]]

    val graph = MutableGraph.fromVertexAndEdgeList(vertices, edges)

    val discoveryTimes = MutableMap.empty[MutableNode[T], Long]
    // Discovery time of the earliest discovered ancestor
    val earliestAncestors = MutableMap.empty[MutableNode[T], Long]
    graph.nodes.foreach {n =>
      discoveryTimes += (n -> Long.MaxValue)
      earliestAncestors += (n -> Long.MaxValue)
    }

    // Perform DFS on each non visited node and pop up elements each time a strongly connected component is identified
    val stack = new Stack[MutableNode[T]]
    val nodesInStack = MutableSet.empty[MutableNode[T]]

    var time = 0L

    def isNodeVisited(node: MutableNode[T]): Boolean = discoveryTimes.contains(node) && discoveryTimes(node) < Long.MaxValue

    def dfs(node: MutableNode[T]): Unit = {
      stack.push(node)
      nodesInStack += node
      time += 1
      discoveryTimes(node) = time
      earliestAncestors(node) = time
      node.children.foreach { child =>
        if (!isNodeVisited(child)) {
          // (node -> child) is a tree edge
          dfs(child)
          earliestAncestors(node) = Math.min(earliestAncestors(node), earliestAncestors(child))
        }
        else if (nodesInStack.contains(child)) {
          // (node -> child) is a back edge
          earliestAncestors(node) = Math.min(earliestAncestors(node), discoveryTimes(child))
        }
        // Otherwise 'child' was already added to a SCC
      }

      // Depth-first search tree of 'node' full explored
      if (discoveryTimes(node) == earliestAncestors(node)) {
        // 'node' is the head node of a SCC. The nodes in the 'stack' above it are part of the same SCC
        val scc = ListBuffer.empty[T]
        while (stack.top != node) {
          scc += stack.top.data
          nodesInStack -= stack.top
          stack.pop()
        }
        scc += stack.top.data
        nodesInStack -= stack.top
        stack.pop()
        stronglyConnectedComponents += scc.toList
      }
    }

    graph.nodes.foreach(node => if(!isNodeVisited(node)) dfs(node))

    stronglyConnectedComponents.toList
  }
}
