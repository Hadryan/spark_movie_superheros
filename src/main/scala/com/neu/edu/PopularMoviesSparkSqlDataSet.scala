package com.neu.edu


import java.nio.charset.CodingErrorAction
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.io.Codec
import scala.io.Source
object PopularMoviesSparkSqlDataSet {

  def loadMovies(): Map[Int,String]={
    implicit val codec=Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    var movieNames:Map[Int,String]=Map()

    val lines=Source.fromFile("./Data/ml-100k/u.item").getLines
    for(line<-lines)
      {
        var field=line.split('|')
        if(field.length>1)
          {
            movieNames += (field(0).toInt -> field(1))
          }
      }
    return movieNames
  }

  // Case class so we can get a column name for our movie ID

  final case class Movie(movieID: Int)

  def main (args:Array[String])={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark= SparkSession
      .builder
      .appName("PopularMoviesSparkSqlDataSet")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","file:///C:/sparkSqlTemp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
      .getOrCreate()

    val lines = spark.sparkContext.textFile("./Data/ml-100k/u.data").map(x=>Movie(x.split("\t")(1).toInt))


    // Convert to a DataSet
    import spark.implicits._
    val moviesDS=lines.toDS()
    // SQL-style to sort all movies by popularity

    val topMovieIDs = moviesDS.groupBy("movieID").count().orderBy(desc("count")).cache()

    topMovieIDs.show()

    val top10=topMovieIDs.take(10)
    val names = loadMovies()
    println("====================Top10 Movies===========================")
    for (result<-top10)
      {println(names(result(0).asInstanceOf[Int]) + ":" +result(1))}

    spark.stop()

  }



}
