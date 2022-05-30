import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import utils.{Schemas, Utils}
import Schemas.Cab

import java.io.File


object CabAnalysis {

  val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

  import org.apache.flink.api.scala._

  def parseRecord(data: DataStream[String]): DataStream[Cab] = {
    data
      .map(  new Utils.Splitter(","))
      .map(Schemas.Cab(_))
  }

  def popularLoc(ds: DataStream[Cab]) = {
    val popular = ds
      .map(data => (data.dest_loc, 1))
      .keyBy(_._1)
      .reduce(new ReduceFunction[(String, Int)] {
        override def reduce(value1: (String, Int), value2: (String, Int)): (String, Int) =
          (value1._1, value2._2 +  value1._2)
      })
      .map(e => (1, e._1, e._2))
      .keyBy(_._1)
      .maxBy(2)

    popular.print()
  }

  def avgPaxByPickupLoc(ds: DataStream[Cab]) = {
    val avgPax = ds
      .map(cab => (cab.pickup_loc, cab.passenger_cnt.toInt,1))
      .keyBy(_._1)
      .reduce(
        new ReduceFunction[(String, Int, Int)] {
          override def reduce(value1: (String, Int, Int),
                              value2: (String, Int, Int)): (String, Int, Int) =
            (value1._1, value1._2 + value2._2 / (value1._3 + 1) , value1._3 + 1)
        }
      )

    avgPax.print()
  }

  def avgTripByDriver(ds: DataStream[Cab]) = {
    val avgTrips = ds
      .map(cab => (cab.driver_name, cab.passenger_cnt.toInt, 1 ))
      .keyBy(_._1)
      .reduce(new ReduceFunction[(String, Int, Int)] {
        override def reduce(value1: (String, Int, Int),
                            value2: (String, Int, Int)): (String, Int, Int) = {
          val total = value1._3 + 1
          (value1._1, value1._2 + value2._2 / total, total)
        }
      })

    avgTrips.print()
  }

  def apply(file: File)  = {
    Utils.show(10, file)
    val data = streamEnv.readTextFile(file.getPath)
    val parsed = parseRecord(data)
    val filtered = parsed.filter(_.is_ongoing =="yes")

    popularLoc(filtered)

    avgPaxByPickupLoc(filtered)

    avgTripByDriver(filtered)


    streamEnv.execute("cab-analysis")
  }
}
