import org.apache.flink.api.common.functions.MapFunction

import java.io.File

object Joins {

  import org.apache.flink.api.scala._

  def join(a: File, b: File) = {
    val flinkEnv = ExecutionEnvironment.getExecutionEnvironment

    val fileA = flinkEnv.readTextFile(a.getPath)
    val fileB = flinkEnv.readTextFile(b.getPath)

    fileA.first(10).print()
    fileB.first(10).print()

    println("\n")

    val dsA = fileA map {
      new MapFunction[String, (Int, String)] {
        override def map(value: String): (Int, String) = {
          value.split(",") match {
            case Array(x, y) => (x.toInt, y)
          }
        }
      }
    }

    val dsB = fileB map {
      new MapFunction[String, (Int, String)] {
        override def map(value: String): (Int, String) = {
          value.split(",") match {
            case Array(x, y) => (x.toInt, y)
          }
        }
      }
    }


    //without a join function, you get a JoinDataset of both parts
    val join =  dsA.join(dsB).where(0).equalTo(0)
    join.print()
    //adding a join function to have only one record(ish):)
    dsA.join(dsB)
      .where(0)
      .equalTo(0)
      {
        (left, right) => (left._1, left._2, right._2)
      }
      .print()

  }

}
