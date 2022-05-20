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

  case class Schema(
                    date: String,
                    month: String,
                    cat: String,
                    product: String,
                    qty: Int
                   )
  object Schema {
    def apply(data: Array[String]): Schema = data match {
      case Array(a,b,c,d,e) => Schema(a,b,c,d,e.toInt)
    }
  }

  case class ReduceSchema(
                         month: String,
                         numProducts: Int,
                         totalPrice: Int,
                         avgProfit: Float
                         )

  def show(n: Int,file: File ) = {
    Source.fromFile(file.getPath)
      .getLines()
      .take(n)
      .foreach(println)
    println("\n")
  }

  def reduceByMonth(file: File) = {
    val grouped = readFile(file)
      .map(
        (line: String) => {
          Schema(line.split(","))
        }
      )
    grouped
      .map( s => ReduceSchema(s.month,1, s.qty, s.qty))
      .keyBy(_.month)
      .reduce(reduceFunc)
      .print()

    streamEnv.execute("reduce")
  }

  def reduceFunc: ReduceFunction[ReduceSchema] = {
    (a: ReduceSchema, b: ReduceSchema) => ReduceSchema(
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
