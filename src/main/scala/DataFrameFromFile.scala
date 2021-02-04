import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Encoder, Encoders, Row, SparkSession, types}

object DataFrameFromFile {
  val sc: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkII")
    .getOrCreate()
  def getCustomer: org.apache.spark.sql.DataFrame ={
    val customer = "Customer.txt"
    val Customer_scheme = StructType(Array(
      StructField("CustID", IntegerType, false),
      StructField("Firstname", StringType, false),
      StructField("Lastname", StringType, false),
      StructField("Phone", StringType, true)
    ))
    val Customer = sc.read
      .option("sep", "|")
      .option("header", true)
      .schema(Customer_scheme)
      .csv(customer)
    return Customer
  }
  def getProduct: org.apache.spark.sql.DataFrame ={
    val product = "Product.txt"
    val Product_scheme = StructType(Array(
      StructField("prodID", IntegerType, false),
      StructField("name", StringType, false),
      StructField("type", StringType, false),
      StructField("version", StringType, true),
      StructField("price", IntegerType, false)
    ))
    val Product = sc.read
      .option("sep", "|")
      .option("header", true)
      .schema(Product_scheme)
      .csv(product)
    return Product
  }
  def getSales: org.apache.spark.sql.DataFrame ={
    val sales = "Sales.txt"
    val Sales_scheme = StructType(Array(
      //txID|custID|prodID|timestamp|amount|quantity
      StructField("txID", IntegerType, false),
      StructField("custID", IntegerType, false),
      StructField("prodID", IntegerType, false),
      StructField("timestamp", StringType, false),
      StructField("amount", IntegerType, false),
      StructField("quantity", IntegerType, false)
    ))
    val Sales = sc.read
      .option("sep", "|")
      .option("header", true)
      .schema(Sales_scheme)
      .csv(sales)
    return Sales
  }
  def getRefund: org.apache.spark.sql.DataFrame ={
    val refund = "Refund.txt"
    val Refund_scheme = StructType(Array(
      //refID|txID|prodID|timestamp|amount|quantity
      StructField("refID", IntegerType, false),
      StructField("txID", IntegerType, false),
      StructField("custID", IntegerType, false),
      StructField("prodID", IntegerType, false),
      StructField("timestamp", StringType, false),
      StructField("amount", IntegerType, false),
      StructField("quantity", IntegerType, false)
    ))
    val Refund = sc.read
      .option("sep", "|")
      .option("header", true)
      .schema(Refund_scheme)
      .csv(refund)
    return Refund
  }
  def main(args:Array[String]) = {
    import sc.implicits._
    this.getCustomer.show(5)
    this.getCustomer.printSchema()
    this.getProduct.show(5)
    this.getProduct.printSchema()
    this.getSales.show(5)
    this.getSales.printSchema()
    this.getRefund.show(5)
    this.getRefund.printSchema()
    //while(true){}
  }
}
