package utils

import java.util.UUID

object Schemas {

  case class StreamSchema(
                   id: String,
                   name: String,
                   qty: Int,
                   time: Long,
                   row_num: Int
                 ) {
    def toCsv: String = {
      s"$id $name $qty $time"
    }
  }
  object StreamSchema {
    def apply(data: Array[String]): StreamSchema = data match {
      case Array(id, name, qty, time, row_num) =>
        new StreamSchema(id,name, qty.toInt, time.toLong, row_num.toInt)
    }
  }


  case class MonthAvgSchema(
                             month: String,
                             numProducts: Int,
                             totalPrice: Int,
                             avgProfit: Float
                           )

  case class BuySchema(
                        date: String,
                        month: String,
                        cat: String,
                        product: String,
                        qty: Int
                      )

  object BuySchema {
    def apply(data: Array[String]): BuySchema = data match {
      case Array(a, b, c, d, e) => BuySchema(a, b, c, d, e.toInt)
    }

    def toTuple(buySchema: BuySchema) = buySchema match {
      case BuySchema(a, b, c, e, f) => (a, b, c, e, f)
    }

  }

  case class Cab(
                  id: String,
                  number: String,
                  cab_type: String,
                  driver_name: String,
                  is_ongoing: String,
                  pickup_loc: String,
                  dest_loc: String,
                  passenger_cnt: String
                )

  object Cab {
    def apply(data: Array[String]): Cab = data match {
      case Array(id, number, cab_type, driver_name, is_ongoing, pickup_loc, dest_loc, passenger_cnt) =>
        Cab(id, number, cab_type, driver_name, is_ongoing, pickup_loc, dest_loc, passenger_cnt)
    }
  }

}
