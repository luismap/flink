import org.apache.flink.api.java.utils.ParameterTool
import utils.{Tokenizer, Utils}
import my_akka_utils.GenerateRandomNum
import java.io.File

object Flink {

  def tokenizeFile(file: File) = {
    Tokenizer.tokenize(file)
  }

  def joinFiles(a: File, b: File) = {
    Joins.join(a,b)
  }

  def main(args: Array[String]): Unit = {

    //val files = Utils.listFiles(ParameterTool.fromArgs(args).get("input"))

    val files = Utils.listFiles("resources")
    //tokenizeFile(files("text.txt"))
    //joinFiles(files("location"), files("person"))

    //DataStreamFromSocket()

    //DSreduce(files("avg20"))

   // Aggregations.minTuples(files("avg20"))
    //Aggregations.minSchema(files("avg20"))
    //Aggregations.minBySchema(files("avg20"))

    GenerateRandomNum(new File("randomInts"))

    Split(files("randomInts"))

    println("back to main")

  }

}