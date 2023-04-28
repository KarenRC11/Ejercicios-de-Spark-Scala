package com.sundogsoftware.spark

import org.apache.spark.sql._
import org.apache.log4j._

object DataFrameDataset {

  case class Person(id:Int, name:String, age:Int, friends:Int)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder()
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()

    //Load each line of the source data into a Dataset
    import spark.implicits._
    val people = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/fakefriends.csv")
      .as[Person]  //Aca lo que esta haciendo es convertir el dataframe a dataset para mejor rendimiento

    //There are lots of other ways to make a DataFrame
    //For example, spark.read.json("json file path")
    //or sqlContext.table("Hive table name")

    println("Here is our inferred schema")
    people.printSchema()

    println("Filter out anyone over 21:")
    people.filter(people("age") < 21).show()

    println("Group by age:")
    people.groupBy("age").count()show()

    println("Make everyone 10 years older:")
    people.select(people("name"), people("age") +10).show()

    spark.stop()

  }
}
