package watermark

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows}
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import utils.{GenerateTcpData, Schemas, Utils}

object WaterMarkDemo {

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
        WatermarkStrategy
          .forGenerator( ctx => new BounderOutOfOrderGenerator())
          .withTimestampAssigner(new SerializableTimestampAssigner[Schemas.StreamSchema] {
            override def extractTimestamp(element: Schemas.StreamSchema, recordTimestamp: Long): Long = element.time
          })
      )

    /**
     *
     */
    val window = withWaterMark
      .map(stream => (stream.time, stream.name, stream.row_num, 1))
      .keyBy(_._2)
      .window(EventTimeSessionWindows.withGap(Time.seconds(1)))
      .reduce(new ReduceFunction[(Long, String, Int, Int)] {
        override def reduce(value1: (Long, String, Int, Int), value2: (Long, String, Int, Int)): (Long, String, Int, Int) =
          (value1._1, value1._2, value1._3 + value2._3, value1._4 + value2._4)
      })




    window.print()

    streamEnv.execute("aboutWindows")

  }

  /**
   *This generator generates watermarks assuming that elements arrive out of order,
   * but only to a certain degree. The latest elements for a certain timestamp t will arrive
   * at most n milliseconds (maxOutOfOrderness) after the earliest elements for timestamp t.
   *
   * this is the same as the watermarkstrategy forBoundedOutOfOrderness
   */
  class BounderOutOfOrderGenerator extends WatermarkGenerator[Schemas.StreamSchema] {
    val maxOutOfOrderness:Long = 1000L
    var currentMaxTimestamp: Long = _

    override def onEvent(event: Schemas.StreamSchema, eventTimestamp: Long, output: WatermarkOutput): Unit =
      currentMaxTimestamp = Math.max(eventTimestamp, currentMaxTimestamp)

    override def onPeriodicEmit(output: WatermarkOutput): Unit =
    // emit the watermark as current highest timestamp minus the out-of-orderness bound
      output.emitWatermark(new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1))
  }

}
