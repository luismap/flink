import Split.streamCtx
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

import java.io.File
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.DurationInt

object Split {

  val streamCtx = StreamExecutionEnvironment.getExecutionEnvironment
  streamCtx.setParallelism(1)
  streamCtx.enableCheckpointing(1000)
  streamCtx.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
  streamCtx.getCheckpointConfig.setCheckpointStorage("file:///tmp/flink-checkpoint")
  val config = new Configuration()
  config.set[java.lang.Boolean](ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true)
  streamCtx.configure(config)


  def main(args: Array[String]): Unit = {

    apply(new File("resources/randomInts"))
  }

  def apply(file: File) = {
    val splitter = new Split(file)
    val ds = splitter.readStreamFromFile
    splitter.writeToFileSink(
      splitter.split(ds)
    )



    streamCtx.execute("split")
  }
}

class Split(file: File) extends Serializable {

  import org.apache.flink.api.scala._

  def readStreamFromFile: DataStream[Int] = {
    streamCtx.readTextFile(file.getPath).map(
      _.toInt
    )
  }

  def isEven(num: Int): Boolean = num % 2 == 0

  def split(dataStream: DataStream[Int]): (DataStream[Int], DataStream[Int]) = {
    val even = dataStream.filter(isEven(_))
    val odd = dataStream.filter(!isEven(_))
    (even, odd)
  }

  def writeToFileSink(streams: (DataStream[Int], DataStream[Int])) = {
    val (even, odd) = streams
    val outputPath = "resources/out"
    val sink: StreamingFileSink[Int] = StreamingFileSink
      .forRowFormat(new Path(outputPath), new SimpleStringEncoder[Int]("UTF-8"))
      .withRollingPolicy(
        DefaultRollingPolicy.builder()
          .withMaxPartSize(1024)
          .build())
      .build()


    even.addSink(sink)
    odd.addSink(sink)
  }
}