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

  test("Strongly connected component identification") {
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
    workflow = new Workflow[TestScheduling] {
      val vertices = workflow.vertices
      val edges = workflow.edges ++ Set((job, job1, TestDependencyDescriptor()))
    }
    workflow = new Workflow[TestScheduling] {
      val vertices = workflow.vertices + job3
      val edges = workflow.edges ++ Set((job2, job3, TestDependencyDescriptor()))
    }
    workflow = new Workflow[TestScheduling] {
      val vertices = workflow.vertices
      val edges = workflow.edges ++ Set((job1, job3, TestDependencyDescriptor()))
    }

    //   ┌───────→ job3
    //   │           ↑
    //   │           |
    //   ├─→ job ← job2 ← job4     singletonJob
    //   │    |             ↑
    //   │    └─────────────┘
    //   │
    //   └── job1 ← job5
    //        |       ↑
    //        └───────┘

    val SCCs = graph.findStronglyConnectedComponents[Job[TestScheduling]](
      workflow.vertices,
      workflow.edges.map { case (child, parent, _) => parent -> child }
    )
    assert(SCCs.size === 4)
    assert(SCCs.filter(_.size == 1).toSet === Set(List(singletonJob), List(job3)))
    assert(SCCs.find(_.size == 2).get.toSet === Set(job1, job5))
    assert(SCCs.find(_.size == 3).get.toSet === Set(job, job2, job4))
  }
}
