package state

import org.apache.flink.api.common.state.{MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeHint}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.util.Collector
import utils.GenerateFromFileTcpData
import utils.Schemas
import utils.Utils.Splitter

import java.io.File

/**
 * example of broadcast, where we will exclude
 * records incoming from broadcasted stream
 * to count number of employee per dept excluding broadcasted employees
 */
object MyBroadCastState {

  val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment


  def initGenerators() = {
    val broadCastDataGen = new Thread(() =>
      GenerateFromFileTcpData("resources/broadcast_small", 9091, 200))
    broadCastDataGen.start()
    val generateData = new Thread(() =>
      GenerateFromFileTcpData("resources/broadcast.txt", 9092, 50)
    )
    generateData.start()

  }

  private val excludeEmployeesDescriptor: MapStateDescriptor[String, Schemas.EmployeeData] =
    new MapStateDescriptor[String, Schemas.EmployeeData]("exludeEmployee", classOf[String], classOf[Schemas.EmployeeData])

  def apply() = {

    import org.apache.flink.streaming.api.scala._

    initGenerators()

    val toBroadcast = streamEnv
      .socketTextStream("localhost", 9091)
      .map(new Splitter(","))
      .map(Schemas.EmployeeData(_))

    val data = streamEnv
      .socketTextStream("localhost", 9092)
      .map(new Splitter(","))
      .map(Schemas.EmployeeData(_))

    val broadcastedEmployeeStream = toBroadcast.broadcast(excludeEmployeesDescriptor)

    data
      .keyBy((s: Schemas.EmployeeData) => s.department)
      .connect(broadcastedEmployeeStream)
      .process(new ExcludeEmployeeProcessor())
      .print()

    streamEnv.execute("broadcastExample")
  }

  class ExcludeEmployeeProcessor
    extends KeyedBroadcastProcessFunction[String, Schemas.EmployeeData, Schemas.EmployeeData, (String, Int, Int)] {

    private var cnt: ValueState[Int] = _
    private var totalCnt: ValueState[Int] = _


    override def open(parameters: Configuration): Unit = {
      cnt = getRuntimeContext.getState(
        new ValueStateDescriptor[Int]("cnt", classOf[Int])
      )
      totalCnt = getRuntimeContext.getState(
        new ValueStateDescriptor[Int]("totalCnt", classOf[Int])
      )
    }

    override def processElement(value: Schemas.EmployeeData,
                                ctx: KeyedBroadcastProcessFunction[String, Schemas.EmployeeData, Schemas.EmployeeData, (String, Int, Int)]#ReadOnlyContext,
                                out: Collector[(String, Int, Int)]): Unit = {
      val currentCnt = cnt.value()
      val currentTotal = totalCnt.value() + 1
      totalCnt.update(currentTotal)
      if (!ctx.getBroadcastState(excludeEmployeesDescriptor).contains(value.employee_id)) {
        out.collect((value.department, currentCnt + 1, currentTotal))
        cnt.update(currentCnt + 1)
      }

    }

    override def processBroadcastElement(value: Schemas.EmployeeData,
                                         ctx: KeyedBroadcastProcessFunction[String, Schemas.EmployeeData, Schemas.EmployeeData, (String, Int, Int)]#Context,
                                         out: Collector[(String, Int, Int)]): Unit = {
      ctx.getBroadcastState(excludeEmployeesDescriptor).put(value.employee_id, value)
    }
  }

}
