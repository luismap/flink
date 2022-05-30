import utils.Schemas.BuySchema
import utils.Utils.{Splitter, show}
import org.apache.flink.api.java.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import utils.Utils

import java.io.File
import scala.collection.parallel.Splitter

object Aggregations {
  import org.apache.flink.api.scala._

  val flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment

  /**
   * with min, flink only keeps info about the compare field,
   * and will get the rest of the info from memory(in case of tuples)
   * (use minBy, if you need to keep consistency)
   * @param file
   * @return
   */
  def minTuples(file: File) = {
    show(50, file)
    flinkEnv
      .readTextFile(file.getPath)
      .map( (a: String)  => BuySchema( new Utils.Splitter(",").map(a)))
      .map(BuySchema.toTuple(_))
      .map(t => (t._2,t._5))
      .keyBy(0)
      .min(1)
      .print()

    flinkEnv.execute("aggregation")
  }

  /**
   * with min, flink only keeps info about the compare field,
   * and will get the rest of the info from memory
   * (use minBy, if you need to keep consistency)
   * @param file
   * @return
   */
  def minSchema(file: File) = {
    show(50, file)
    flinkEnv
      .readTextFile(file.getPath)
      .map( (a: String)  => BuySchema( new Utils.Splitter(",").map(a)))
      .keyBy(_.month)
      .min("qty")
      .print()

    flinkEnv.execute("aggregation")
  }


  /**
   * minby will keep consistency
    * @param file
   * @return
   */
  def minBySchema(file: File) = {
    show(50, file)
    flinkEnv
      .readTextFile(file.getPath)
      .map( (a: String)  => BuySchema( new Utils.Splitter(",").map(a)))
      .keyBy(_.month)
      .minBy("qty")
      .print()

    flinkEnv.execute("aggregation")
  }
}
