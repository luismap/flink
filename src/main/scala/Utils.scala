import org.apache.flink.api.common.functions.MapFunction

import java.io.File
import scala.io.Source

object Utils {

  def listFiles(path: String): Map[String, File] = {
    val content = new File(path)
    val files = content.listFiles().filter(!_.isDirectory)
    Map(files map {
      (file: File) =>
        Tuple2(
          file.getName,
          file
        )
    }: _*)
  }

  def show(n: Int,file: File ) = {
    Source.fromFile(file.getPath)
      .getLines()
      .take(n)
      .foreach(println)
    println("\n")
  }

  class Splitter(del: String) extends MapFunction[String, Array[String]]
    {
      override def map(value: String): Array[String] =
        value.split(del)
    }

}
