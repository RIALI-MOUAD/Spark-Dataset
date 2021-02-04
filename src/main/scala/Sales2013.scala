import DataFrameFromFile._
import org.apache.spark.sql.SparkSession


object Sales2013 {
  val sc : SparkSession  = sc

   def AreSales2013(Timestamp: String): Boolean = {
    Timestamp.contains("2013")
  }
  def main(args: Array[String]): Unit = {
    val Sales = getSales
    val Sales2013 = Sales.filter(Row => AreSales2013(Row(3).toString))
    Sales2013.show()
    val amount= Sales2013.select("amount").rdd.map(r=>r(0).asInstanceOf[Int]).collect()
    println(amount.sum)
  }
}
