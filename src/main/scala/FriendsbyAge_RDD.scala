package con.sundogsoftware.spark

import org.apache.spark._
import org.apache.log4j._

object FriendsbyAge_RDD {

  def parseLine(line:String): (Int, Int) = {

    val fields = line.split(",")

    val age = fields(2).toInt
    val numFriends = fields(3).toInt

    (age, numFriends)

  }

  def main(args:Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsbyAge")

    val lines = sc.textFile("data/fakefriends-noheader.csv")

    val rdd = lines.map(parseLine)

    //Lots going on here
    //We are starting with an RDD of form (age, numFriends) where age is the KEY and numFriends is the VALUE
    //We use mapValues to convert each numFriends value to a tuple of (numFriends, 1)
    //Then we use reduceByKey to sump up the total numFriends and total instances for each age, by
    //adding together all the numFriends values and 1's respectively.
    val totalsByAge = rdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

    //So now we have tuples of (age, totalFriends, totalInstances))
    //To compute the average we divide totalFriends / totalInstance for each age.
    val averageByAge = totalsByAge.mapValues(x=> x._1 / x._2)

    //Collect the results from the RDD (This kicks off computing the DAG and actually executes the job)
    val results = averageByAge.collect()

    //Sort and print the final results.
    results.sorted.foreach(println)

  }



}
