package com.criteo.cuttle

import org.scalatest.FunSuite

class WorkflowSpec extends FunSuite with TestScheduling {

  test("We should build a valid DAG for our workflow") {
    val jobs = Vector.tabulate(4)(i => Job(i.toString, testScheduling)(completed))
    val graph = (jobs(1) and jobs(2)) dependsOn jobs(0) dependsOn jobs(3)

    assert(graph.vertices.size == 4)
    assert(graph.edges.size == 3)
  }

  test("Serialize workflow DAG in linear representation should throw an exception when DAG has a cycle") {
    val job1 = Job("job1", testScheduling)(completed)
    val job2 = Job("job2", testScheduling)(completed)
    val job3 = Job("job3", testScheduling)(completed)

    val job = Job("job", testScheduling)(completed)

    val workflow = job dependsOn (job1 and job2) dependsOn job3 dependsOn job1

    intercept[IllegalArgumentException] {
      workflow.jobsInOrder
    }
  }

  test("Serialize workflow DAG in linear representation should be ok without cycles") {
    val job1 = Job("job1", testScheduling)(completed)
    val job2 = Job("job2", testScheduling)(completed)
    val job3 = Job("job3", testScheduling)(completed)

    val job = Job("job", testScheduling)(completed)

    val workflow = job dependsOn (job1 and job2) dependsOn job3

    assert(workflow.jobsInOrder.size === 4)
  }

}
