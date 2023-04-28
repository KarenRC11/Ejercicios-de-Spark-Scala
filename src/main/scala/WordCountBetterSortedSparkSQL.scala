package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.functions._

object WordCountBetterSortedSparkSQL {

  case class Book(value: String)

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    //Read each line of my book into a Dataset
    import spark.implicits._
    val input = spark.read.text("dara/book.txt").as[Book]

    val words = input
      .select(explode(split($"value", "\\W+")).alias("word"))
      .filter($"word" =!= "")

    val lowercaseWords = words.select(lower($"word").alias("word"))

    val wordCounts = lowercaseWords.groupBy("word").count()

    val wordCountSorted = wordCounts.sort("count")

    wordCountSorted.show(wordCountSorted.count.toInt)

  }

}
