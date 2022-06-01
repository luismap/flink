package state

import org.apache.flink.api.common.functions.{ReduceFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.functions.FirstReducer
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector
import utils.{GenerateTcpData, Schemas, Utils}


object MyReduceState {

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

  /**
   * ReducingState
   * This keeps a single value that represents the aggregation of all values added to the state.
   * The interface is similar to ListState but elements added using add(T) are reduced to an aggregate
   * using a specified ReduceFunction
   */
  class StatefulCount extends RichFlatMapFunction[Schemas.StreamSchema, (String, Int, String)] {
    private var sum: ValueState[(String, Int)] = _
    private var rowNumsSum: ReducingState[Int] = _
    private var cnt: ValueState[Int] = _


    override def flatMap(value: Schemas.StreamSchema, out: Collector[(String, Int, String)]): Unit = {
      val currentValue = if (sum.value() != null) sum.value() else ("", 0)
      val currentCnt = cnt.value()


      if (currentCnt >= 10) {
        out.collect((currentValue._1, currentValue._2, rowNumsSum.get().toString))
        cnt.clear()
        sum.clear()
        rowNumsSum.clear()

      } else {
        sum.update((value.name, value.row_num))
        cnt.update(currentCnt + 1)
        rowNumsSum.add(value.row_num)
      }

    }

    override def open(parameters: Configuration): Unit = {
      sum = getRuntimeContext.getState(
        new ValueStateDescriptor[(String, Int)]("sum", createTypeInformation[(String, Int)])
      )
      cnt = getRuntimeContext.getState(
        new ValueStateDescriptor[Int]("cnt", createTypeInformation[Int])
      )

      rowNumsSum = getRuntimeContext.getReducingState(
        new ReducingStateDescriptor[Int]("rowNumsSum", new ReduceFunction[Int] {
          override def reduce(value1: Int, value2: Int): Int =
            value1 + value2
        }, classOf[Int])
      )
    }
  }
}
