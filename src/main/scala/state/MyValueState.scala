package state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector
import utils.{GenerateTcpData, Schemas, Utils}

object MyValueState {

  val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

  def apply() = {

    val generator = new Thread(() => GenerateTcpData())
    generator.start()

    val data = streamEnv.socketTextStream("localhost", 9090, ' ')


    data
      .map(new Utils.Splitter(" "))
      .map(Schemas.StreamSchema(_))
      .keyBy(_.name)
      .flatMap(new StatefulCount())
      .print()

    streamEnv.execute("value-state")

  }

  class StatefulCount extends RichFlatMapFunction[Schemas.StreamSchema, (String, Seq[Int], Int)] {
    //state name (sum)
    private var sum: ValueState[(String, Seq[Int], Int)] = _
    private var cnt: ValueState[Int] = _


    override def flatMap(value: Schemas.StreamSchema, out: Collector[(String, Seq[Int], Int)]): Unit = {
    val currentValue = if ( sum.value() != null) sum.value() else ("",Seq.empty[Int],0)
    val currentCnt = cnt.value()


    if (currentCnt >= 10){
      val a = currentValue._3 - 10
      val b = currentValue._3
      val ans = (10 * (a + b))/2
      out.collect((currentValue._1, currentValue._2, ans))
      sum.clear()
      cnt.clear()

    } else {
      sum.update((value.name, currentValue._2 :+ value.row_num, value.row_num))
      cnt.update(currentCnt +  1)
    }

    }

    override def open(parameters: Configuration): Unit = {
      sum = getRuntimeContext.getState(
        new ValueStateDescriptor[(String,Seq[Int], Int)]("sum", createTypeInformation[(String,Seq[Int], Int)])
      )
      cnt = getRuntimeContext.getState(
        new ValueStateDescriptor[Int]("cnt", createTypeInformation[Int])
      )
    }
  }
}
