package com.neu.edu

import org.apache.log4j._
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
object SparkSqlFriends {
 case class Person (ID:Int, name:String, age:Int,numFrriends:Int)

  def mapper(line:String): Person={
    val fields=line.split(',')
    val person:Person=Person(fields(0).toInt,fields(1),fields(2).toInt,fields(3).toInt)
    return person
  }

  def main (args:Array[String])={
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Spark Session Interface in Spark 2
    val spark=SparkSession
      .builder()
      .appName("SparkSqlFriends")
      .master("local[*]")
      .config("spark.sql.warehouse.dir", "file:///C:/sparkSqlTemp") // Create a temp folder it is a Necessary to work around a Windows bug in Spark 2.0.0; omit if not on Windows.
      .getOrCreate()

    // Convert csv file to a DataSet, using  Person case
    val lines=spark.sparkContext.textFile("./Data/fakefriends.csv")
    val people=lines.map(mapper)
// SQL commands
    print ("-----------------------SQL commands-------------------------")

    // Infer the schema, and register the DataSet as a table.
    import spark.implicits._
    val schemaPeople = people.toDS
    println("Here is our inferred schema:")
    schemaPeople.printSchema()
    // replace the context of dataset into table view called people
    schemaPeople.createOrReplaceTempView("people")
    // SQL can be run over DataFrames that have been registered as a table
    println("Print all teneager")
    val tenager=spark.sql("Select * from people where age>=13 and age<=19")
    val result=tenager.collect()
    result.foreach(println)

// SQL fucntion
    print ("-----------------------SQl function--------------------------")

    val peoplefucntion = people.toDS().cache()

    println("Here is our inferred schema:")
    peoplefucntion.printSchema()

    println("Let's select the name column:")
    peoplefucntion.select("name").show()

    println("Filter out anyone over 21:")
    peoplefucntion.filter(peoplefucntion("age")>21).show()

    println("Group by age:")
    peoplefucntion.groupBy("age").count().show()

    println("Make everyone 10 years older:")
    peoplefucntion.select(peoplefucntion("name"),peoplefucntion("age")+10).show()

    spark.stop() // stop spark session

  }

}
