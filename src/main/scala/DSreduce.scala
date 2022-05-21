import Schemas.{BuySchema, MonthAvgSchema}
import Utils.show
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.io.File
import scala.io.Source

object DSreduce {

  val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

  import org.apache.flink.api.scala._

  def readFile(file: File): DataStream[String] = {
    streamEnv.readTextFile(file.getPath)
  }

  def reduceByMonth(file: File) = {
    val grouped = readFile(file)
      .map(
        (line: String) => {
          BuySchema(line.split(","))
        }
      )
    grouped
      .map(s => MonthAvgSchema(s.month, 1, s.qty, s.qty))
      .keyBy(_.month)
      .reduce(reduceFunc)
      .print()

    streamEnv.execute("reduce")
  }

  def reduceFunc: ReduceFunction[MonthAvgSchema] = {
    (a: MonthAvgSchema, b: MonthAvgSchema) =>
      MonthAvgSchema(
        a.month,
        a.numProducts + b.numProducts,
        a.totalPrice + b.totalPrice,
        a.totalPrice + b.totalPrice / (a.numProducts + 1)
      )
  }

  def apply(file: File) = {
    show(5, file)
    reduceByMonth(file)
  }
}
