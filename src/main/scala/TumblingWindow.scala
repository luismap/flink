import org.apache.flink.api.common.eventtime.{AscendingTimestampsWatermarks, SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import utils.{GenerateTcpData, Schemas, Utils}

import java.time.Duration
import scala.collection.parallel.Splitter
import scala.concurrent.duration.DurationInt

object TumblingWindow {

  val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

  def apply(): Unit = {

    val thread = new Thread(() => GenerateTcpData())
    thread.start()

    import org.apache.flink.api.scala._
    val readFromSocket = streamEnv.socketTextStream("localhost", 9090, ' ')

    val withWaterMark = readFromSocket
      .map(record => new Utils.Splitter(" ").map(record))
      .map(Schemas.StreamSchema(_))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forMonotonousTimestamps[Schemas.StreamSchema]()
          .withTimestampAssigner(new SerializableTimestampAssigner[Schemas.StreamSchema] {
            override def extractTimestamp(element: Schemas.StreamSchema, recordTimestamp: Long): Long = element.time
          })
      )

    val window = withWaterMark
      .map(stream => (stream.time, stream.name, stream.qty))
      .keyBy(_._2)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .reduce(new ReduceFunction[(Long, String, Int)] {
        override def reduce(value1: (Long, String, Int), value2: (Long, String, Int)): (Long, String, Int) =
          (value1._1, value1._2, value1._3 +  value2._3)
      })



    window.print()

    streamEnv.execute("aboutWindows")

  }
}
