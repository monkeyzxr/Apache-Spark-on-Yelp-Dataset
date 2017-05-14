/**
  * Created by monkeyzxr on 2017/3/24.
  */

import org.apache.spark.SparkContext._
import org.apache.spark.{SparkConf, SparkContext}

object QuestionThree {
  def main(args: Array[String]): Unit = {
    val inputFilePath = "file:///Users/monkeyzxr/Desktop/" +
                        "CS 6350.001 - Big Data Management and Analytics/Assignment/" +
                        "Homework-2-partB/TestQ4"

    val conf = new SparkConf().setAppName("QuestionThree").setMaster("local")
    val sc = new SparkContext(conf)
    val lines = sc.textFile(inputFilePath)   //get lines as RDD

    val header = lines.first()
    val data = lines.filter(line => line != header) //remove header line

    //create key-value pairs of target-weight
    val kv = data.map(_.split("\\s+")).map(items => (items(1), items(2).toInt)).cache()

    // print out the results
    val results = kv.reduceByKey(_+_).collect().foreach(println)
  }

}
