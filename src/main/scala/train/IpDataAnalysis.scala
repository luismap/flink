package train

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.operators.Order
import org.apache.flink.util.Collector
import utils.Utils.Splitter
import utils.Schemas

import java.io.File

object IpDataAnalysis {

  import org.apache.flink.api.scala._ //important to import depending if you use streaming or dataset

  val streamEnv = ExecutionEnvironment.getExecutionEnvironment


  def readData(file: File) = {
    streamEnv.readTextFile(file.getPath).map(new Splitter(","))
  }

  def parseData(ds: DataSet[Array[String]]) = {
    ds
      .map(Schemas.IpSchema(_))
  }

  def totalClicks(ds: DataSet[Schemas.IpSchema]) = {
    ds
      .map((s: Schemas.IpSchema) => (s.website, 1))
      .groupBy(t => t._1)
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) =
          (value1._1, value1._2 + value2._2)
      })
  }

  def maxNumbClicks(ds: DataSet[Schemas.IpSchema]) = {

    ds
      .map(s => Tuple2(s.website, 1))
      .groupBy(0)
      .sum(1)
      .maxBy(1)
  }

  def minNumbClicks(ds: DataSet[Schemas.IpSchema]) = {

    ds
      .map(s => Tuple2(s.website, 1))
      .groupBy(0)
      .sum(1)
      .minBy(1)
  }

  def distinctUserByWebsite(ds: DataSet[Schemas.IpSchema]) = {
    ds
      .map(s => (s.website, s.user_id))
      .groupBy(0)
      .sortGroup(1, Order.ASCENDING)
      .reduceGroup(
        (in: Iterator[(String, String)], out: Collector[(String, Int, Int)]) => {
          val users = in.toSeq
          out.collect(users.head._1, users.size, users.toSet.size)
        }
      )
  }


  def apply(file: File) = {
    val data = readData(file)
    val parsed = parseData(data)
    val tClicks = totalClicks(parsed)
    val maxClick = maxNumbClicks(parsed)
    val minClick = minNumbClicks(parsed)
    val distUsers = distinctUserByWebsite(parsed)

    println("TOTAL CLICKS")
    tClicks.print()
    println("MAX CLICKS")
    maxClick.print()
    println("MIN CLICKS")
    minClick.print()
    println("DISTINCT USERS")
    distUsers.print()
    //parsed.executeAndCollect(10).foreach(println)


    //    streamEnv.execute("ipAnalysis")
  }
}
