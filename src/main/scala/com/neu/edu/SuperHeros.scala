package com.neu.edu
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import org.apache.log4j._
import scala.collection.mutable.ArrayBuffer

object SuperHeros {

  //fn to extract the hero id and number of connections from each lines
  def countCoOccuracne(line:String)={
    var elements=line.split("\\s+")
    (elements(0).toInt,elements.length-1)
  }

  def parseNames(line:String): Option[(Int,String)]={
    var fields=line.split("\"")
    if(fields.length>1)
      {
        return Some(fields(0).trim.toInt,fields(1))
      }
      else None
  }

  def main(args:Array[String]): Unit ={
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","SuperHeros")

    //Build  hero ID-> name RDD
    val names=sc.textFile("./Data/marvel-names.txt")
    val namesRdd=names.flatMap(parseNames)

    // load siperhero coappearacne data
    val lines = sc.textFile("./Data/marvel-graph.txt")

    // Convert to (heroID, number of connections) RDD
    val pairings=lines.map(countCoOccuracne)
    // Combine entries that span more than one line

    val totalFriendsByCharacter = pairings.reduceByKey( (x,y) => x + y )

    // Flip it to # of connections, hero ID
    val flipped=totalFriendsByCharacter.map(x=>(x._2,x._1))

    // Find the max # of connections
    val mostPopular = flipped.max()
    // Look up the name (lookup returns an array of results, so we need to access the first result with (0)).
    val mostPopularName = namesRdd.lookup(mostPopular._2)(0)

    // Print out our answer!
    println(s"$mostPopularName is the most popular superhero with ${mostPopular._1} co-appearances.")



  }


}
