package com.criteo.langoustinepp

import org.scalatest.FunSuite

class LangoustinePPSpec extends FunSuite {
  test("Graph building") {
    object TestScheduling extends Scheduling {
      type Context = Unit
      type DependencyDescriptor = Unit
      val defaultDependencyDescriptor = ()
    }
    type TestScheduling = TestScheduling.type

    val jobs =
      (0 to 3).map (i => Job[TestScheduling](i.toString))
    val graph = (jobs(1) and jobs(2)) dependsOn jobs(0) dependsOn jobs(3)
    assert(graph.vertices.size == 4)
    assert(graph.edges.size == 3)
  }
}
