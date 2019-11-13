package com.criteo.cuttle.examples
import java.io.{File, PrintWriter}

import com.criteo.cuttle.Completed
import com.criteo.cuttle.cron._

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

    val cronJobP1 = CronJobPart(id = "jobpart1", name = "Job Part 1", description = "This is job part 1") { implicit e =>
    Future {
      val value = rand.nextInt(100)
      writeIntToFile(value, fileNamea1)
      System.out.println(s"Job part 1 wrote $value.")
      e.streams.info(s"Job part 1 wrote $value.")
      Completed
    }}

    val cronJobP2 = CronJobPart(id = "jobpart2", name = "Job Part 2", description = "This is job part 2") { implicit e =>
      Future {
        val value = rand.nextInt(100)
        writeIntToFile(value, fileNamea2)
        System.out.println(s"Job part 2 wrote $value.")
        e.streams.info(s"Job part 2 wrote $value.")
        Completed
      }}

    val cronJobP3 = CronJobPart(id = "jobpart3", name = "job part 3", description = "This is job part 3") { implicit e =>
      Future {
        val x = readIntFromFile(fileNamea1)
        val y = readIntFromFile(fileNamea2)
        val value = x + y
        System.out.println(s"$x + $y = $value.")
        e.streams.info(s"$x + $y = $value.")
        Completed
      }}

    val cronJobP4 = CronJobPart(id = "jobpart4", name = "job part 4", description = "This is job part 4") { implicit e =>
      Future {
        val x = readIntFromFile(fileNamea1)
        val y = readIntFromFile(fileNamea2)
        val value = x * y
        System.out.println(s"$x * $y = $value.")
        e.streams.info(s"$x * $y = $value.")
        Completed
      }}

    val cronJobP5 = CronJobPart(id = "jobpart5", name = "job part 5", description = "This is job part 5") { implicit e =>
      Future {
        new File(fileNamea1).delete()
        new File(fileNamea2).delete()
        Completed
      }}


    val fileNameb = "letter.log"
    val cronJobP6 = CronJobPart(id = "jobpart6", name = "Job Part 6", description = "This is job part 6") { implicit e =>
      Future {
        val value = rand.nextString(5)
        writeStringToFile(value, fileNameb)
        System.out.println(s"Job part 6 wrote $value.")
        e.streams.info(s"Job part 6 wrote $value.")
        Completed
      }}


    val cronJobP7 = CronJobPart(id = "jobpart7", name = "job part 7", description = "This is job part 7") { implicit e =>
      Future {
        val value = readStringFromFile(fileNameb)
        System.out.println(s"Job Part 7 read $value.")
        e.streams.info(s"Job Part 7 read $value.")
        Completed
      }}

    val cronJobP8 = CronJobPart(id = "jobpart8", name = "job part 8", description = "This is job part 8") { implicit e =>
      Future {
        new File(fileNameb).delete()
        Completed
      }}

    val workload = CronWorkload(
      Set(CronJob(id="cronjoba", name="Cron Job A",
          CronScheduling("0-59/10 * * ? * *", 1),
          cronJobP5 dependsOn (cronJobP3 and cronJobP4) dependsOn (cronJobP1 and cronJobP2)),
        CronJob("cronjobb", "Cron Job 2 B", CronScheduling("0-59/15 * * ? * *"),
          cronJobP8 dependsOn cronJobP7 dependsOn cronJobP6)))


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
