package evictors

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import utils.{GenerateTcpData, Schemas, Utils}

object Evictor {

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

    /**
     * Evictor
     * - can be use to create an operation
     * before or/and after a window function has been executed
     *
     * Predefined evictors (by default operate in before window func execution):
     *  - CountEvictor
     *  - DeltaEvictor
     *  - TimeEvictor
     *
     *  Your own Evictor, impliment Evictor Interface
     */
    val window = withWaterMark
      .map(stream => (stream.time, stream.name, stream.row_num, 1))
      .keyBy(_._2)
      .window(GlobalWindows.create())
      .trigger(CountTrigger.of(10))
      .evictor(CountEvictor.of(6))
      .reduce(new ReduceFunction[(Long, String, Int, Int)] {
        override def reduce(value1: (Long, String, Int, Int), value2: (Long, String, Int, Int)): (Long, String, Int, Int) =
          (value1._1, value1._2, value1._3 + value2._3, value1._4 + value2._4)
      })




    window.print()

    streamEnv.execute("aboutWindows")

  }
}
