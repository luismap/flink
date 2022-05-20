import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object DataStreamFromSocket {

  import org.apache.flink.api.scala._
  //create stream environment
  val flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment

  def readFromSocket: DataStream[String] = {
    /**
     * when creating the socket, use the loop back address 127.0.0.1 instead of
     * of the name(some connections errors will occur otherwise
     */
    flinkEnv.socketTextStream("localhost", 9091)
  }

  def handleData(data: DataStream[String]) = {
    data.map {
      (a: String) => {
        val data = a.split(" ")
        data match {
          case Array(a:String,b:String) => (a,b)
          case Array(a:String,b: String, _* ) => (a,b)
          case _ => (null,null)
        }

      }
    }

  }

  def apply() = {
    handleData(
      readFromSocket
    ).print()

    flinkEnv.execute("socketStream1")
  }
}
