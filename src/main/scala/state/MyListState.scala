package state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.util.Collector
import utils.{GenerateTcpData, Schemas, Utils}

object MyListState {

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
   * ListState<T>: This keeps a list of elements. You can append elements and retrieve an Iterable over
   * all currently stored elements. Elements are added using add(T) or addAll(List<T>), the Iterable can
   * be retrieved using Iterable<T> get(). You can also override the existing list with update(List<T>)
   */
  class StatefulCount extends RichFlatMapFunction[Schemas.StreamSchema, (String, Int, String)] {
    private var sum: ValueState[(String, Int)] = _
    private var rowNums: ListState[Int] = _ //if you want to use an Iterator
    private var cnt: ValueState[Int] = _


    override def flatMap(value: Schemas.StreamSchema, out: Collector[(String, Int,String)]): Unit = {
      val currentValue = if (sum.value() != null) sum.value() else ("", 0)
      val currentCnt = cnt.value()


      if (currentCnt >= 10) {
        out.collect((currentValue._1, currentValue._2, rowNums.get().toString))
        sum.clear()
        cnt.clear()
        rowNums.clear()

      } else {
        sum.update((value.name, value.row_num))
        cnt.update(currentCnt + 1)
        rowNums.add(value.row_num)
      }

    }

    override def open(parameters: Configuration): Unit = {
      sum = getRuntimeContext.getState(
        new ValueStateDescriptor[(String, Int)]("sum", createTypeInformation[(String, Int)])
      )
      cnt = getRuntimeContext.getState(
        new ValueStateDescriptor[Int]("cnt", createTypeInformation[Int])
      )

      rowNums = getRuntimeContext.getListState(
        new ListStateDescriptor[Int]("rowNums", classOf[Int])
      )
    }
  }
}
