// Example: Hello cuttle!

// This a very simple cuttle project using the time series scheduler
// to execute a bunch of shell commands
package com.criteo.cuttle.examples

// The main package contains everything needed to create
// a cuttle project.
import com.criteo.cuttle._

// The local platform allows to locally fork some processes
// (_here we will just fork shell commands_).
import com.criteo.cuttle.platforms.local._

// We will use the time series scheduler for this project.
import com.criteo.cuttle.timeseries._

// We also have to import the Java 8 time API, used by the
// time series scheduler.
import java.time.ZoneOffset.UTC
import java.time._

object HelloWorld {

  // A cuttle project is just embeded into any Scala application.
  def main(args: Array[String]): Unit = {

    // We define a common start date for all our jobs. This is required by the
    // time series scheduler to define a start date for each job. Here we dynaically
    // compute it as 7 days ago (_and we round it to midnight UTC_).
    val start: Instant = LocalDate.now.minusDays(7).atStartOfDay.toInstant(UTC)

    // Here is our first job. The second parameter is the scheduling configuration.
    // __hello1__ is defined as a job computing hourly partitions starting at the start
    // date declared before.
    val hello1 =
      Job("hello1", hourly(start), "Hello 1") {
        // The side effect function takes the execution as parameter. The execution
        // contains useful meta data as well as the __context__ which is basically the
        // input data for our execution.
        implicit e =>
          // Because this job uses a time series scheduling configuration the context
          // contains the information about the time partition to compute, ie the start
          // and end date.
          val partitionToCompute = (e.context.start) + "-" + (e.context.end)

          // The `sh` interpolation is provided by the local platform. It allows us to
          // declare a bash command to execute. If you want use many commands you can combine them
          // in a for comprehension.
          for {
            _ <- sh"echo Hello for $partitionToCompute".exec()
            _ <- sh"echo Check my project page at https://github.com/criteo/cuttle".exec()
            completed <- sh"sleep 1".exec()
          } yield completed
      }

    // Our second job is also on hourly job. Nothing special here.
    // We still can do pretty sophisticated things with sh commands.
    // For example you can create an executable file and run it.
    val hello2 =
      Job("hello2", hourly(start), "Hello 2") { implicit e =>
        val scriptName = s"/tmp/run${e.id}"
        for {
          _ <- sh"echo Looping for 20 seconds...".exec()
          _ <- sh"""
                |echo "
                |    number=\$$1
                |    shift
                |    for n in \$$(seq \$$number); do
                |      sleep 1
                |      \$$@
                |    done
                |" > /tmp/run${e.id}
              """.exec()
          _ <- sh"chmod +x $scriptName".exec()
          _ <- sh"$scriptName 20 date".exec()
          completed <- sh"echo OK".exec()
        } yield completed
      }

    // Here is our third job. Look how we can also define some metadata such as a human friendly
    // name and a set of tags. This information is used in the UI to help retrieving your jobs.
    val hello3 =
      Job("hello3", hourly(start), "Hello 3", tags = Set(Tag("unsafe job"))) { implicit e =>
        val complete = for {
          _ <- sh"echo Hello 3".exec()
          completed <- sh"sleep 3".exec()
        } yield completed

        complete.map { _ =>
          // We generate an artifical failure if the partition is for 2 days ago between 00 and 01
          // and if the `/tmp/hello3_success` file does not exist.
          if (e.context.start == LocalDate.now.minusDays(2).atStartOfDay.toInstant(UTC)
              && !new java.io.File("/tmp/hello3_success").exists) {
            e.streams.error("Oops, please create the /tmp/hello3_success file to make this execution pass...")

            // Throwing an execption is enough to fail the execution, but you can also return
            // a failed Future.
            sys.error("Oops!!!")
          } else {

            // The completed value is returned to cuttle to announce the job execution as
            // successful. In this case the time series scheduler will mark the partition as
            // successful for job __hello3__ and store this information in his internal state.
            Completed
          }
        }
      }

    // Our last job is a daily job. For the daily job we still need to annouce a start date, plus
    // we need to define the time zone for which _days_ must be considered. The partitions for
    // daily jobs will usually be 24 hours, unless you are choosing a time zone with light saving.
    val world = Job("world", daily(UTC, start), "World") { implicit e =>
      for {
        _ <- sh"echo World".exec()
        completed <- sh"sleep 6".exec()
      } yield completed
    }

    // Finally we bootstrap our cuttle project.
    CuttleProject("Hello World", env = ("Demo", false)) {
      // Any cuttle project contains a Workflow to execute. This Workflow is composed from jobs
      // or from others smaller Workflows.
      world dependsOn (hello1 and hello2 and hello3)
    }.
    // The call to start actually start the scheduler and open an HTTP port serving both an UI to
    // track the project status as well as JSON API.
    start()
  }
}
