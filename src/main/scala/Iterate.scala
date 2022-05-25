import org.apache.commons.math3.analysis.function.StepFunction
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.connector.source.lib.NumberSequenceSource
import org.apache.flink.api.java.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Iterate {

  val env = ExecutionEnvironment.getExecutionEnvironment


  def apply() = {

    val initial = env.generateSequence(0L, 5L).map(_.toInt).map(
      new MapFunction[Int, (Int, Int)] {
        override def map(value: Int): (Int, Int) =
          (value, 1)
      }
    )

    initial.print()

    val iterate = initial.iterate(9)


    val feed = iterate.map(
      new MapFunction[(Int, Int), (Int, Int)] { //do not convert to lambdas, compiler fails to get types
        override def map(value: (Int, Int)): (Int, Int) =
          (value._1, value._2 + 1)
      })

    val result = iterate.closeWith(feed)

    result.print()

    val init = env.fromElements(0).map(_.toInt).iterate(10000)


    val feed2 = init.map(new MapFunction[Int, Int] {
      override def map(value: Int): Int = {
        val x = Math.random()
        val y = Math.random()
        value + (if (x * x + y * y < 1) 1 else 0)
      }
    })


    val result2:DataSet[Int] = init.closeWith(feed2)

    result2.map(new MapFunction[Int, Double] {
      override def map(count: Int): Double = {
        count / 10000.toDouble * 4
      }

    }).print


  }

}
