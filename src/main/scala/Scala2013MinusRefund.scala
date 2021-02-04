import DataFrameFromFile._
import Sales2013._


object Scala2013MinusRefund {
  //val sc :SparkSession = sc

  def main(args: Array[String]): Unit = {
    val sales2013 =getSales.filter(Row => AreSales2013(Row(3).toString))
    val refundGrouped = getRefund.groupBy("txID").count()
    val SalesMinusRefund= sales2013.alias("s")
      .join(refundGrouped,sales2013("txID")===refundGrouped("txID"),"leftanti")
    SalesMinusRefund.show(30)
    println(s"${SalesMinusRefund.select("amount")
      .rdd.map(r=>r(0).asInstanceOf[Int])
      .collect().sum} is the total amount of sales not refunded")
  }
}
