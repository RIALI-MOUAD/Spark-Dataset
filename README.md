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
### build.sbt :
In this kind of projects. We have always to set up the"[build.sbt]()" file, which contains in this case :
```scala
name := "SparkII"
version := "1.0"
scalaVersion := "2.12.13"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
```
As it is obvious, the [build.sbt][df1] contains :
  - The project name
  - sbt version : 1.0
  - Scala version : 2.12.13
  - libraryDependencies : here I only used one dependency which is "org.apache.spark"%%"spark-sql" % "3.0.1" 
