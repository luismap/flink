import Utils.Splitter

import scala.collection.parallel.Splitter

object Schemas {

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
}
