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

  test("Cycle identification") {
    val job1 = Job("job1", testScheduling)(completed)
    val job2 = Job("job2", testScheduling)(completed)
    val job3 = Job("job3", testScheduling)(completed)
    val job4 = Job("job4", testScheduling)(completed)
    val job5 = Job("job5", testScheduling)(completed)

    val job = Job("job", testScheduling)(completed)
    val singletonJob = Job("singleton_job", testScheduling)(completed)

    val cycle1 = job dependsOn job2 dependsOn job4 dependsOn job
    val cycle2 = job1 dependsOn job5 dependsOn job1
    var workflow = cycle1 and cycle2 and singletonJob
    workflow = ((workflow withDependencyFrom job1 to job) withDependencyFrom job3 to job2) withDependencyFrom job3 to job1

    val cycles = workflow.findCycles()
    assert(cycles.size === 2)

    assert(cycles.find(cycle => cycle.size == 3)
      .getOrElse(throw new Exception("Expected to find a cycle with 3 jobs"))
      .map(_.id).toSet == Set(job.id, job2.id, job4.id))

    assert(cycles.find(cycle => cycle.size == 2)
      .getOrElse(throw new Exception("Expected to find a cycle with 2 jobs"))
      .map(_.id).toSet == Set(job1.id, job5.id))

    intercept[IllegalArgumentException] {
      workflow.jobsInOrder
    }
  }
}
