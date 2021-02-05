# Spark Resilient Distributed Dataset
This project contains 4 sub-projects using sbt (scala build tools) :
  - Sales Distribution : evaluate the amount and quantity for each product name.
  - Sales 2013 : total amount of sales.
  - Sales 2013 : total amount of sales, except refund(remboursés).
  - User Products : total number of products for each customer.
## Datasets :
This project is based on 4 different datasets which are 4 text files with the delimiter "|" : 
- [Customer.txt](https://github.com/RIALI-MOUAD/RIALI-MOUAD-Spark-Resilient-Distributed-Dataset/blob/main/Customer.txt) 
- [Product.txt](https://github.com/RIALI-MOUAD/RIALI-MOUAD-Spark-Resilient-Distributed-Dataset/blob/main/Product.txt) 
- [Sales.txt](https://github.com/RIALI-MOUAD/RIALI-MOUAD-Spark-Resilient-Distributed-Dataset/blob/main/Sales.txt) 
- [Refund.txt](https://github.com/RIALI-MOUAD/RIALI-MOUAD-Spark-Resilient-Distributed-Dataset/blob/main/Refund.txt) 
![alt text](https://github.com/RIALI-MOUAD/RIALI-MOUAD-Spark-Resilient-Distributed-Dataset/blob/main/data%20warehouse.png)

## Building Project !
### General structure :
```bash
.:
build.sbt     Product.txt  README.md   Sales.txt          src
Customer.txt  project      Refund.txt  Scala_project.pdf

./project:
build.properties  project

./project/project:

./src:
main  test

./src/main:
scala

./src/main/scala:
DataFrameFromFile.scala  SalesDistribution.scala     UserProducts.scala
Sales2013.scala          Scala2013MinusRefund.scala

./src/test:
scala

./src/test/scala:
```

### build.sbt :
In this kind of projects. We have always to set up the"[build.sbt]()" file, which contains in this case :
```scala
name := "SparkII"
version := "1.0"
scalaVersion := "2.12.13"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
```
As it is obvious, the [build.sbt]() contains :
  - The project name
  - sbt version : 1.0
  - Scala version : 2.12.13
  - libraryDependencies : here I only used one dependency which is "org.apache.spark"%%"spark-sql" % "3.0.1" 
  
### From textFile to DataFrame :

To build the project,first we have to think about :
> How to generate DataFrame from textFile ?

The answer that I have chosen is to create a Scala object which does the job, I called it [DataFrameFromFile]():
```scala
object DataFrameFromFile {
  val sc: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("SparkII")
    .getOrCreate()
  def getCustomer: org.apache.spark.sql.DataFrame ={...}
  def getProduct: org.apache.spark.sql.DataFrame ={...}
  def getSales: org.apache.spark.sql.DataFrame ={...}
  def getRefund: org.apache.spark.sql.DataFrame ={...}
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
```

Besides the immutable variable [sc] which generates  "[SparkSession](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/SparkSession.html)" Object, I created 4 functions, each one of them generates a specefic [DataFrame](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/index.html#DataFrame=org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]) : Customer, Product, Sales, Refund.

Let's take the example of getCustomer:
```scala
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
```
While [Customer.txt]() is like : 
```
CustID|Firstname|Lastname|Phone
815001|LYNCH|JACKSON|14157317623
815002|MOSS|FIELDS|14156874907
815003|MCCALL|BOYLE|14151620323
815004|DELACRUZ|MADDEN|14151364678
815005|RUIZ|CAIN|14155195074
...
```

After defining the path to the textFile, I set a schema to control the inputs of the Dataset by creating a [StructType](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/types/StructType.html) object using Array<[StructField(name: String, dataType: DataType, nullable: Boolean = true)](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/types/StructField.html)>  

Then, I called [read.csv(/path/to/file)](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/SparkSession.html#read:org.apache.spark.sql.DataFrameReader) with the following options :
* "sep" : "|" => the separator
* "header" : true => The first line contains columns names

As result , I got the following output :
```
+------+---------+--------+-----------+
|CustID|Firstname|Lastname|      Phone|
+------+---------+--------+-----------+
|815001|    LYNCH| JACKSON|14157317623|
|815002|     MOSS|  FIELDS|14156874907|
|815003|   MCCALL|   BOYLE|14151620323|
|815004| DELACRUZ|  MADDEN|14151364678|
|815005|     RUIZ|    CAIN|14155195074|
+------+---------+--------+-----------+
only showing top 5 rows

root
 |-- CustID: integer (nullable = true)
 |-- Firstname: string (nullable = true)
 |-- Lastname: string (nullable = true)
 |-- Phone: string (nullable = true)
```
