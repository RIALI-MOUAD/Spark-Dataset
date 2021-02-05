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

### Sub-Projects :

#### 1- Sales Distribution :

##### Description :
This sub-project is meant to evaluate the amount and quantity for each product name.
##### Employed Datasets :
- Product.txt
- Sales.txt
##### Objects :
###### DataFrameFromFile.scala :
The reason behind calling this object is to generate the Dataframes that we'll employ later to serve the project role as we are going to see in the main project object.
###### SalesDistribution.scala : 
The main object of this sub-project which looks like this :
```scala
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
```

> How does it work ?

First I defined Two immutable variables **Sales** and **Products**. Each presents an "*org.apache.spark.sql.DataFrame*" object, generated of this two functions herited from the **DataFrameFromFile** object :
```scala 
      val Sales = /*This function*/getSales
      val Products =/*This Function*/getProduct
```
The next step is setting a new DataFrame based on  *Sales* Grouped By Foreign Key : *prodID* , by summing the values of those following columns : **amount** *&* **quantity** :
```scala
var SalesDistr =Sales.groupBy("prodID").sum("amount","quantity")
```

Finally, we call the *[join]()* method to do an **inner join** between *SalesDistr* and *Products* without forgetting to select the following columns :
> - name : product name
> - prodID : product ID
> - Total amount : Total amount of each of sold products
> - Total quantity : Total quantity of each of sold products


#### Final Result :
> ## Voila !!

```
+------+--------------------+------------+--------------+
|prodID|                name|Total amount|Total quantity|
+------+--------------------+------------+--------------+
|  1127|        Memory card |       12901|           679|
|  1125|           Keyboard |       68112|           688|
|  1114|           Harddisk |       74943|           757|
|  1122|      Kids's tablet |       59103|           597|
|  1130|       Baby Monitor |      223011|           639|
|  1113|            Desktop |      949095|           655|
|  1128|            Speaker |      101022|           678|
|  1132|      Car Connector |      168392|           776|
|  1119|          Camcorder |      195546|           654|
|  1124|              Drone |       63991|           719|
|  1123|    VR Play Station |      139499|           701|
|  1121|            Monitor |      108174|           726|
|  1118|             Camera |      332334|           666|
|  1116|             Tablet |      517260|           740|
|  1111|              Phone |      339034|           566|
|  1126|        Magic Mouse |       70488|           712|
|  1131|    Doorbell Carema |      132057|           603|
|  1115|      GamingConsole |      167328|           672|
|  1120|            Printer |      261052|           748|
|  1129|Home Automation Kit |      632367|           633|
+------+--------------------+------------+--------------+
only showing top 20 rows
```



#### 2- Sales 2013 :

##### Description :
This sub-project is meant to calculate total amount of sales happened in *2013*.
##### Employed Datasets :
- Sales.txt
##### Objects :
###### DataFrameFromFile.scala :
The reason behind calling this object is to generate the Dataframes that we'll employ later to serve the project role as we are going to see in the main project object.
###### Sales2013.scala : 
The main object of this sub-project which looks like this :
```scala
object Sales2013 {
  //val sc : SparkSession  = sc

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
```

> How does it work ?

First I defined 1 immutable variable **Sales** which presents an "*org.apache.spark.sql.DataFrame*" object, generated of this  function herited from the **DataFrameFromFile** object :
```scala 
      val Sales = /*This function*/getSales
```
Then I define the function **AreSales2013** that returns a Boolean which tells if the *Timestamp* variable contains *2013* or not :
```scala
   def AreSales2013(Timestamp: String): Boolean = {
    Timestamp.contains("2013")
  }
```
In addition to that, I create a new DataFrame *Sales2013* wihch contains only the sales happened in 2013 by applying a filter on the intial *Sales* DataFrame :
```scala
    val Sales2013 = Sales.filter(Row => AreSales2013(Row(3).toString))
```
After we got the *Sales2013* DataFrame , We are interested just in the *amount* column, so I isolate it by movin its values to an *Array<Int>* :  
  
```scala
    val amount= Sales2013.select("amount").rdd.map(r=>r(0).asInstanceOf[Int]).collect()
```

Finally, I calculate the Total amount by applying the *sum* mrthod on the *Array<Int>* that we got in the previous step :
  
``` scala
    println(amount.sum)
```
#### Final Result :
> ## Voila !!
```
+------+------+------+-------------------+------+--------+
|  txID|custID|prodID|          timestamp|amount|quantity|
+------+------+------+-------------------+------+--------+
|115005|815159|  1116|02/11/2013 03:25:52|  2796|       4|
|115007|815113|  1129|04/30/2013 11:44:30|   999|       1|
|115010|815290|  1119|04/13/2013 10:40:38|  1495|       5|
|115024|815497|  1121|04/19/2013 06:36:21|   149|       1|
|115027|815019|  1112|05/20/2013 14:02:04|  4995|       5|
|115033|815348|  1115|01/12/2013 11:21:23|  1245|       5|
|115036|815368|  1122|04/07/2013 15:48:58|   396|       4|
|115039|815316|  1121|02/17/2013 02:35:35|   298|       2|
|115042|815497|  1123|01/28/2013 13:43:26|   398|       2|
|115043|815344|  1123|02/21/2013 07:16:25|   597|       3|
|115046|815095|  1121|04/30/2013 21:15:05|   298|       2|
|115048|815201|  1126|05/09/2013 11:25:12|   396|       4|
|115054|815070|  1111|01/23/2013 10:21:51|  1198|       2|
|115056|815151|  1116|04/03/2013 13:22:27|  2796|       4|
|115057|815045|  1116|05/21/2013 15:06:51|   699|       1|
|115059|815277|  1126|02/17/2013 05:39:06|    99|       1|
|115060|815282|  1120|04/29/2013 06:23:18|   698|       2|
|115063|815404|  1111|04/18/2013 19:54:18|  2396|       4|
|115067|815340|  1116|05/24/2013 20:21:29|  2097|       3|
|115068|815485|  1120|03/23/2013 01:16:07|  1745|       5|
+------+------+------+-------------------+------+--------+
only showing top 20 rows
```
And the Total amount of sales in 2013 is :

```
1637540
```

#### 3- Sales 2013 (Except Refund):

##### Description :
This sub-project is meant to calculate total amount of sales happened in *2013* , except refund(remboursés).
##### Employed Datasets :
- Sales.txt
- Refund.txt
##### Objects :
###### DataFrameFromFile.scala :
The reason behind calling this object is to generate the Dataframes that we'll employ later to serve the project role as we are going to see in the main project object.
###### Sales2013.scala :
In this project we'll use the function *AreSales2013* to drop non useful data 

###### Scala2013MinusRefund.scala : 
The main object of this sub-project which looks like this :
```scala
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
```

> How does it work ?

First I defined Two immutable variables **sales2013** and **refundGrouped**. Each presents an "*org.apache.spark.sql.DataFrame*" object, generated of this two functions herited from the **DataFrameFromFile** and **Sales2013** objects :

```scala 
    val sales2013 =getSales.filter(Row => AreSales2013(Row(3).toString))
    val refundGrouped = getRefund.groupBy("txID").count()
```
Then I define the DataFrame **SalesMinusRefund** that contains the data in **sales2013** that do not appear in **refundGrouped** by ensuring a *leftanti* join using the foreign key *txID* between those last DataFrames  :
```scala
 val SalesMinusRefund= sales2013.alias("s").join(refundGrouped,sales2013("txID")===refundGrouped("txID"),"leftanti")
```
After we got the *SalesMinusRefund* DataFrame , We are interested just in the *amount* column, so I isolate it by movin its values to an *Array<Int>* and calculate the sum of all of its elements:  
  
```scala
  println(s"${SalesMinusRefund.select("amount")
      .rdd.map(r=>r(0).asInstanceOf[Int])
      .collect().sum} is the total amount of sales not refunded")
```

#### Final Result :
> ## Voila !!
```
+------+------+------+-------------------+------+--------+
|  txID|custID|prodID|          timestamp|amount|quantity|
+------+------+------+-------------------+------+--------+
|115005|815159|  1116|02/11/2013 03:25:52|  2796|       4|
|115007|815113|  1129|04/30/2013 11:44:30|   999|       1|
|115010|815290|  1119|04/13/2013 10:40:38|  1495|       5|
|115024|815497|  1121|04/19/2013 06:36:21|   149|       1|
|115027|815019|  1112|05/20/2013 14:02:04|  4995|       5|
|115033|815348|  1115|01/12/2013 11:21:23|  1245|       5|
|115036|815368|  1122|04/07/2013 15:48:58|   396|       4|
|115042|815497|  1123|01/28/2013 13:43:26|   398|       2|
|115043|815344|  1123|02/21/2013 07:16:25|   597|       3|
|115046|815095|  1121|04/30/2013 21:15:05|   298|       2|
|115048|815201|  1126|05/09/2013 11:25:12|   396|       4|
|115054|815070|  1111|01/23/2013 10:21:51|  1198|       2|
|115056|815151|  1116|04/03/2013 13:22:27|  2796|       4|
|115057|815045|  1116|05/21/2013 15:06:51|   699|       1|
|115059|815277|  1126|02/17/2013 05:39:06|    99|       1|
|115060|815282|  1120|04/29/2013 06:23:18|   698|       2|
|115063|815404|  1111|04/18/2013 19:54:18|  2396|       4|
|115067|815340|  1116|05/24/2013 20:21:29|  2097|       3|
|115068|815485|  1120|03/23/2013 01:16:07|  1745|       5|
|115072|815361|  1112|03/14/2013 03:59:11|  1998|       2|
|115084|815028|  1124|03/02/2013 22:08:42|   178|       2|
|115087|815370|  1112|04/10/2013 05:39:56|  2997|       3|
|115092|815050|  1116|02/23/2013 23:36:53|   699|       1|
|115094|815428|  1132|01/02/2013 11:04:29|   651|       3|
|115097|815149|  1129|05/01/2013 09:39:45|  1998|       2|
|115101|815036|  1128|03/01/2013 09:30:35|   745|       5|
|115120|815207|  1120|02/23/2013 11:59:01|  1396|       4|
|115124|815155|  1132|03/02/2013 22:07:46|   651|       3|
|115127|815444|  1118|03/31/2013 10:02:57|  1996|       4|
|115130|815209|  1127|04/16/2013 18:51:48|    19|       1|
+------+------+------+-------------------+------+--------+
only showing top 30 rows
```

And the Total amount is : 

```
1489232 is the total amount of sales not refunded
```
### 4-  User products:

##### Description :
This sub-project is meant to calculate total number of products for each customer.
##### Employed Datasets :
- Sales.txt
- Customer.txt
##### Objects :
###### DataFrameFromFile.scala :
The reason behind calling this object is to generate the Dataframes that we'll employ later to serve the project role as we are going to see in the main project object.

###### UserProducts.scala : 
The main object of this sub-project which looks like this :
```scala
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
```

> How does it work ?

First I defined Two immutable variables **Customer** and **Sales**. Each presents an "*org.apache.spark.sql.DataFrame*" object, generated of this two functions herited from the **DataFrameFromFile** object :

```scala 
    val Customer = getCustomer
    val Sales = getSales
```
Then I define the DataFrame **SalesperCustomerID** that contains the data in **Sales** Grouped by customer Id "custID" and the sum of quantity :
```scala
    val SalesperCustomerID = Sales.groupBy("custID").sum("quantity")
```
Next, I create the DataFrame **SalesperCustomer** by joining ***SalesperCustomerID*** and ***Sales*** in order to get additional infos about the customer (first name, last name ...):  
  
```scala
    val SalesperCustomer = SalesperCustomerID.alias("SId")
      .join(Customer.alias("c"),Customer("custID")===SalesperCustomerID("custID"),"inner")
      .select(col("c.custID"),
        col("c.Firstname"),
        col("c.Lastname"),
        col("SId.sum(quantity)").as("Total number of products"))
```

#### Final Result :
> ## Voila !!

Finally we got :
```
+------+---------+--------+------------------------+
|custID|Firstname|Lastname|Total number of products|
+------+---------+--------+------------------------+
|815304|   MCNEIL|   ESTES|                      36|
|815397|    KNAPP| PERKINS|                      33|
|815158|    MEJIA| HIGGINS|                      31|
|815181|     REID|  MURPHY|                      14|
|815486|  SWEENEY|  HOWELL|                      22|
|815051|   FIELDS|      HO|                      37|
|815334|   HORTON|    KANE|                      29|
|815272|   WERNER|   WEEKS|                      29|
|815497| SHEPHERD|   HUANG|                      26|
|815404|  STANTON|   JAMES|                      22|
|815187|    WEBER| VAZQUEZ|                      31|
|815119|   HUNTER|  TURNER|                      23|
|815137|   HOLMES|TRUJILLO|                      37|
|815219|    BARRY|  DUNCAN|                      29|
|815338|    BROCK| BURGESS|                      19|
|815302|    MOSES|   BROCK|                      38|
|815068|   VARGAS|  WAGNER|                      18|
|815253|   DORSEY|   COWAN|                      20|
|815336|     ROSS|SANTIAGO|                      35|
|815085|   PRINCE|  CAMPOS|                      43|
+------+---------+--------+------------------------+
only showing top 20 rows
```
