import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import java.io.File

object Tokenizer {
  import org.apache.flink.api.scala._

  def tokenize(file: File) = {

    val flinkEnv = ExecutionEnvironment.getExecutionEnvironment

    val text: DataSet[String] = flinkEnv.readTextFile(file.getPath)

    val filtered = text.filter(
      _.startsWith("n")
    )

    val tokenize = filtered map {
      new MapFunction[String, (String, Int)] {
        override def map(value: String): (String, Int) =
          Tuple2(value, 1)
      }
    }

    tokenize.print()

    tokenize.groupBy(0).sum(1).print()

    //flinkEnv.execute()
  }

}
