package com.criteo.cuttle.examples

import com.criteo.cuttle._
import timeseries._

object HelloWorld {

  def main(args: Array[String]): Unit = {
    val start = date"2017-04-06T00:00Z"
    val tagPolite = Tag("Polite", Some("Jobs that greet you politely"), Some("#27ae60"))
    val tagPure = Tag("Pure", Some("Jobs with no side effects"), Some("#3498db"))
    val tagLRCheck = Tag("LRCheck", Some("Jobs that check last runs"), Some("#95a5a6"))
    val tagBigFilter = Tag(name = "BigFilter", color= Some("#e74c3c"))
    val tagCharacter = Tag(name = "Character", color= Some("#8e44ad"))
    val tagItalian = Tag(name = "Italian", color= Some("#e67e22"))
    val tagLiatiel = Tag(name = "Liatel", color= Some("#f1c40f"))

    val hello1 = Job("1", Some("Hello 1"), Some("The Hello Job 1"), Set(tagPolite, tagLRCheck), hourly(start)) { implicit e =>
      sh"""
        echo "${e.context} -> Hello";
        sleep 3
      """.exec()
    }

    val hello2 = Job("2", Some("Hello 2"), Some("The Hello Job 2"), Set(tagPolite, tagLRCheck, tagPure), hourly(start)) { implicit e =>
      sh"""
        echo "${e.context} -> Hello";
        sleep 3
      """.exec()
    }

    val hello3 = Job("3", Some("Hello 3"), Some("The Hello Job 3"), Set(tagPolite), hourly(start)) { implicit e =>
      sh"""
        echo "${e.context} -> Hello";
        sleep 3
      """.exec()
    }

    val hello4 = Job("4", Some("Hello 4"), Some("The Hello Job 4"), Set(tagPolite), hourly(start)) { implicit e =>
      sh"""
        echo "${e.context} -> Hello";
        sleep 3
      """.exec()
    }

    val world = Job("world", Some("World"), Some("The World Job"), Set(tagBigFilter), daily("UTC", start)) { implicit e =>
      sh"""
        echo "${e.context} -> World";
        sleep 1
      """.exec()
    }

    val coma = Job("comaJob", Some(","), Some("The World Job"), Set(tagBigFilter, tagPolite), daily("UTC", start)) { implicit e =>
      sh"""
        echo "${e.context} -> World";
        sleep 1
      """.exec()
    }

    val excl = Job("exclJob", Some("!"), Some("The Excl Job"), Set(tagBigFilter, tagPure), daily("UTC", start)) { implicit e =>
      sh"""
        echo "${e.context} -> World";
        sleep 1
      """.exec()
    }

    val JB = Job("jbJob", Some("Jay Bee"), Some("The Jay Bee Job"), Set(tagItalian, tagPolite), daily("UTC", start)) { implicit e =>
      sh"""
        echo "${e.context} -> World";
        sleep 1
      """.exec()
    }

    val giulio = Job("italianJob", Some("Giulio"), Some("Giulio's Job"), Set(tagCharacter, tagPolite, tagLRCheck), daily("UTC", start)) { implicit e =>
      sh"""
        echo "${e.context} -> World";
        sleep 1
      """.exec()
    }

    val notFound = Job("notFound", Some("Not Foond"), Some("NF"), Set(tagLiatiel, tagItalian), daily("UTC", start)) { implicit e =>
      sh"""
        echo "${e.context} -> World";
        sleep 1
      """.exec()
    }

    Cuttle("Hello World") {
      (((JB and (notFound dependsOn giulio)) dependsOn coma) and excl) dependsOn (world dependsOn (hello1 and hello2 and hello3 and hello4))
    }.run()
  }

}
