import DataFrameFromFile._
import org.apache.spark.sql.functions.col




object SalesDistribution {
  //val sc: SparkSession = sc

    def main(args: Array[String]): Unit = {
      val Sales:org.apache.spark.sql.DataFrame = getSales
      val Products:org.apache.spark.sql.DataFrame=getProduct
      Sales.columns.foreach(println(_))
      var SalesDistr =Sales.groupBy("prodID").sum("amount","quantity")
      SalesDistr = SalesDistr.alias("n").join(Products,SalesDistr("prodID")===Products("prodID"),"inner")
        .select(col("n.prodID"),
          col("name"),
          col("sum(amount)").as("Total amount"),
          col("sum(quantity)").as("Total quantity"))
      SalesDistr.show()
      while(true){}
    }
}
