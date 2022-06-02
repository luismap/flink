package utils

import java.util.UUID
import javax.management.monitor.StringMonitor

object Schemas {

  //user_id,network_name,user_IP,user_country,website, Time spent before next click
  case class EmployeeData(
                           employee_id: String,
                           name: String,
                           role: String,
                           department: String,
                           department_id: String
                         )

  object EmployeeData {
    def apply(data: Array[String]): EmployeeData = data match {
      case Array(eid, name, role, dpt, dptId) =>
        new EmployeeData(eid, name, role, dpt, dptId)
    }
  }

  case class IpSchema(
                       user_id: String,
                       network_name: String,
                       user_ip: Long,
                       user_country: String,
                       website: String,
                       time_spent_seconds: Int,
                     )

  object IpSchema {
    def apply(data: Array[String]): IpSchema = data match {
      case Array(uid, nname, uip, ucntry, website, timespentscnds) =>
        new IpSchema(uid, nname, uip.toLong, ucntry, website, timespentscnds.toInt)
    }
  }

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
        new StreamSchema(id, name, qty.toInt, time.toLong, row_num.toInt)
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
