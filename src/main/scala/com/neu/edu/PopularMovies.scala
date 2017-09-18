package com.neu.edu

import java.nio.charset.CodingErrorAction

import org.apache.log4j._
import org.apache.spark._

import scala.io.{Codec, Source}


/** Find the movies with the most ratings. */
object PopularMovies {

  def loadMovieNames(): Map[Int, String] = {
    //handel Character Encoding
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    //create and map from Ints(ID) to String(Movies Name), and populate with u.items
    var movieNames: Map[Int, String] = Map()

    val lines = Source.fromFile("./Data/ml-100k/u.item").getLines()
    for (line <- lines) {
      var field = line.split('|')
      if (field.length > 1) {
        movieNames += (field(0).toInt -> field(1))
      }
    }
    return movieNames
  }

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)



    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PopularMovies")

    // Create a broadcast variable of our ID -> movie name map
    var nameDict=sc.broadcast(loadMovieNames)

    // Read in each rating line
    val lines = sc.textFile("./Data/ml-100k/u.data")

    // Map to (movieID, 1) tuples
    val movies = lines.map(x => (x.split("\t")(1).toInt, 1))

    // Count up all the 1's for each movie
    val movieCounts = movies.reduceByKey((x, y) => x + y)

    // Flip (movieID, count) to (count, movieID)
    val flipped = movieCounts.map(x => (x._2, x._1))

    // Sort
    val sortedMovies = flipped.sortByKey()

    val sortedMoviesWithNames=sortedMovies.map(x=>(nameDict.value(x._2),x._1))
    // Collect and print results
    val results = sortedMoviesWithNames.collect()

    results.foreach(println)
  }
}


