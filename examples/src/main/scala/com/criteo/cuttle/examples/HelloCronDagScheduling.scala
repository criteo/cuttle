package com.criteo.cuttle.examples
import java.io.{File, PrintWriter}

import com.criteo.cuttle.{Completed, Job}
import com.criteo.cuttle.cron._
import com.criteo.cuttle.cron.CronPipeline._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.Random

object HelloCronDagScheduling {

  // A cuttle project is just embedded into any Scala application.
  def main(args: Array[String]): Unit = {

    val fileNamea1 = "number1.log"
    val fileNamea2 = "number2.log"
    val rand = Random

    val cronJobP1 = Job(id = "job1", name = "Job 1", description = "This is job 1", scheduling = CronScheduling(1)) { implicit e =>
    Future {
      val value = rand.nextInt(100)
      writeIntToFile(value, fileNamea1)
      e.streams.info(s"Job 1 wrote $value.")
      Completed
    }}

    val cronJobP2 = Job(id = "job2", name = "Job 2", description = "This is job 2", scheduling = CronScheduling(2)) { implicit e =>
      Future {
        val value = rand.nextInt(100)
        writeIntToFile(value, fileNamea2)
        e.streams.info(s"Job 2 wrote $value.")
        Completed
      }}

    val cronJobP3 = Job(id = "job3", name = "job 3", description = "This is job 3", scheduling = CronScheduling(3)) { implicit e =>
      Future {
        val x = readIntFromFile(fileNamea1)
        val y = readIntFromFile(fileNamea2)
        val value = x + y
        e.streams.info(s"$x + $y = $value.")
        Completed
      }}

    val cronJobP4 = Job(id = "job4", name = "job 4", description = "This is job 4", scheduling = CronScheduling(4)) { implicit e =>
      Future {
        val x = readIntFromFile(fileNamea1)
        val y = readIntFromFile(fileNamea2)
        val value = x * y
        e.streams.info(s"$x * $y = $value.")
        Completed
      }}

    val cronJobP5 = Job(id = "job5", name = "job 5", description = "This is job 5", scheduling = CronScheduling(5)) { implicit e =>
      Future {
        new File(fileNamea1).delete()
        new File(fileNamea2).delete()
        Completed
      }}


    val fileNameb = "letter.log"
    val cronJobP6 = Job(id = "job6", name = "Job 6", description = "This is job 6", scheduling = CronScheduling(6)) { implicit e =>
      Future {
        val value = rand.nextString(5)
        writeStringToFile(value, fileNameb)
        e.streams.info(s"Job 6 wrote $value.")
        Completed
      }}


    val cronJobP7 = Job(id = "job7", name = "job 7", description = "This is job 7", scheduling = CronScheduling(7)) { implicit e =>
      Future {
        val value = readStringFromFile(fileNameb)
        e.streams.info(s"Job 7 read $value.")
        Completed
      }}

    val cronJobP8 = Job(id = "job8", name = "job 8", description = "This is job 8", scheduling = CronScheduling(8)) { implicit e =>
      Future {
        new File(fileNameb).delete()
        Completed
      }}

    val dag1 = ( cronJobP5 dependsOn (cronJobP3 and cronJobP4) dependsOn (cronJobP1 and cronJobP2)).
      toCronDag("0-59/10 * * ? * *", "dag1")

    val dag2 = (cronJobP8 dependsOn cronJobP7 dependsOn cronJobP6).toCronDag("0-59/15 * * ? * *", "dag2")


    val workload = CronWorkload(Set(dag1, dag2))

    implicit val scheduler = CronScheduler(logger)

    // Project instantiation, it takes an implicit scheduler that we've just defined!
    val project = CronProject(
      name = "Hello Cron Dag Scheduling Example",
      version = "0.0.1",
      description = "My first Cron DAG with Cuttle project"
    )(workload)

    // __Finally start it!__
    project.start()
  }

  def writeIntToFile(value: Int, filePath: String) = {
    val file_Object = new File(filePath)
    val print_Writer = new PrintWriter(file_Object)
    print_Writer.write(value.toString)
    print_Writer.close()
  }

  def writeStringToFile(value: String, filePath: String) = {
    val file_Object = new File(filePath)
    val print_Writer = new PrintWriter(file_Object)
    print_Writer.write(value)
    print_Writer.close()
  }

  def readIntFromFile(filePath: String): Int = {
    val lines = Source.fromFile(filePath).getLines.toList
    Integer.valueOf(lines.head)
  }

  def readStringFromFile(filePath: String): String = {
    val lines = Source.fromFile(filePath).getLines.toList
    lines.head
  }
}
