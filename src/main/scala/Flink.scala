import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import java.io.File
import scala.io.Source

object Flink {

  def tokenizeFile(file: File) = {
    Tokenizer.tokenize(file)
  }

  def joinFiles(a: File, b: File) = {
    Joins.join(a,b)
  }

  def main(args: Array[String]): Unit = {

    val files = Utils.listFiles(ParameterTool.fromArgs(args).get("input"))

    //tokenizeFile(files("text.txt"))
    //joinFiles(files("location"), files("person"))

    //DataStreamFromSocket()

    //DSreduce(files("avg20"))

    Aggregations.minTuples(files("avg20"))
    Aggregations.minSchema(files("avg20"))
    Aggregations.minBySchema(files("avg20"))
  }

}
