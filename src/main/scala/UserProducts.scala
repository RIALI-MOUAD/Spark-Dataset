import DataFrameFromFile._
import org.apache.spark.sql.functions.col



object UserProducts {

  def main(args: Array[String]): Unit = {
    val Customer = getCustomer
    val Sales = getSales
    val SalesperCustomerID = Sales.groupBy("custID").sum("quantity")
    val SalesperCustomer = SalesperCustomerID.alias("SId")
      .join(Customer.alias("c"),Customer("custID")===SalesperCustomerID("custID"),"inner")
      .select(col("c.custID"),
        col("c.Firstname"),
        col("c.Lastname"),
        col("SId.sum(quantity)").as("Total number of products"))
    SalesperCustomer.show()
  }
}
