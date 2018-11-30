// Example: Hello custom scheduling!

// This a minimal cuttle project providing a
// basic scheduler running a single job n times.
package com.criteo.cuttle.examples

// The main package contains everything needed to create
// a cuttle project.
import com.criteo.cuttle._
import com.criteo.cuttle.platforms.local

// The local platform allows to locally fork some processes
// (_here we will just fork shell scripts_).
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

import com.criteo.cuttle.platforms.local._

object HelloCustomScheduling {

  // A cuttle project is just embeded into any Scala application.
  def main(args: Array[String]): Unit = {

    // __Let's define our custom scheduler first!__
    //
    // To define a scheduler we need 4 things:
    //
    // 1. A type representing the execution input parameters. A value
    //    of this type will be passed to each new execution.
    // 2. A type representing the scheduling configuration. Jobs will
    //    be configured with a value of this type.
    // 3. A type representing the workload handled by the scheduler.
    //    It can be as simple as a simple list of job, or as sophisticated
    //    as a DAG with specific configuration on each edge.
    // 4. The scheduling logic itself: the scheduler code. This code
    //    will be provided with the workload and a reference to the cuttle
    //    executor.

    // This is our scheduling context, ie. the data that we will pass
    // as input to new executions.
    case class LoopContext(iteration: Int) extends SchedulingContext {

      // The compare to allow to define the priority of parallel executions
      // operating on different context. Here the lowest iteration is more
      // prioritary
      def compareTo(other: SchedulingContext) = other match {
        case LoopContext(otherIteration) =>
          iteration - otherIteration
      }
    }

    // This our scheduling definition and configuration. For example
    // here we allow to configure the number or times the job must
    // be run successfully.
    case class LoopScheduling(repeat: Int) extends Scheduling {
      type Context = LoopContext
    }

    // This is the representation of our workload. Our scheduler just
    // take a single job to execute. More sophisticated scheduler could
    // for example use a complex workflow of jobs.
    case class LoopJobs(job: Job[LoopScheduling]) extends Workload[LoopScheduling] {
      val all = Set(job)
    }

    // Finally, the scheduler logic itself
    val loopScheduler = new Scheduler[LoopScheduling] {
      def start(jobs: Workload[LoopScheduling], executor: Executor[LoopScheduling], xa: XA, logger: Logger) =
        jobs match {
          case LoopJobs(job @ Job(id, LoopScheduling(repeat), _, _, _)) =>
            logger.info(s"Will run job `${id}' ${repeat} times")

            // Now the loop
            (0 to repeat).foreach { i =>
              // This is the semantic of our scheduler:
              // We will retry the execution until it is successful.
              def runSuccessfully(ctx: LoopContext): Future[Completed] = {
                logger.info(s"Running ${id}.${ctx.iteration}")
                val (_, result) = executor.run(job, ctx)

                result.recoverWith {
                  case e =>
                    logger.error(s"${id}.${ctx.iteration} failed! Retrying...")
                    runSuccessfully(ctx)
                }
              }

              // Here we block the thread as it is a toy implementation,
              // but more sophisticated schedulers could be totally asynchronous
              // to better manage jobs parallelization.
              Await.ready(runSuccessfully(LoopContext(i)), Duration.Inf)
            }
        }
    }

    // __Now let's define a job!__
    val hello =
      Job("hello", LoopScheduling(repeat = 10)) {
        // The side effect function takes the execution as parameter. The execution
        // contains useful meta data as well as the __context__ which is basically the
        // input data for our execution.
        implicit e =>
          // We can read execution parameters from the context
          val i = e.context.iteration

          // Now do some real work in BASH, failing randomly...
          exec"""
            bash -c 'ps -aux
            sds
              sleep $i;
              x=$$((RANDOM % 2));
              echo "Random result is $$x";
              (($$x == 0)) && echo $i || false;
            '
          """ ()
      }

    // Create a connection to the database where the application states are persisted
    val stateDbTransactor = Database.connect(DatabaseConfig.fromEnv)

    // Finally we bootstrap our cuttle project
    val executor = new Executor[LoopScheduling](
      // The local platform is used to execute the bash commands defined in the hello job in a dedicated thread pool
      Seq(local.LocalPlatform(maxForkedProcesses = 10)),
      stateDbTransactor,
      logger,
      projectName = "Custom scheduling example",
      projectVersion = "version"
    )(RetryStrategy.ExponentialBackoffRetryStrategy)

    loopScheduler.start(LoopJobs(hello), executor, stateDbTransactor, logger)
  }
}
