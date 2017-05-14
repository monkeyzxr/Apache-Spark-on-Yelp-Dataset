/**
  * Created by monkeyzxr on 2017/3/24.
  */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object QuestionOne {
  def main(args: Array[String]): Unit = {
    val businessFilePath = "file:///Users/monkeyzxr/Desktop/" +
                           "CS 6350.001 - Big Data Management and Analytics/Assignment/" +
                           "Homework-2-partB/dataset/business.csv"
    val reviewFilePath = "file:///Users/monkeyzxr/Desktop/" +
                         "CS 6350.001 - Big Data Management and Analytics/Assignment/" +
                         "Homework-2-partB/dataset/review.csv"

    //The entry point into all functionality in Spark is the SparkSession class.
    //To create a basic SparkSession, just use SparkSession.builder()
    val spark = SparkSession
      .builder()
      .appName("Question-One")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    //For implicit conversions like converting RDDs to DataFrames
    import spark.implicits._

    /**** work on the business.csv ****/
    // Create an RDD of business
    val businessRDD = spark.sparkContext.textFile(businessFilePath)

    // The schema of business is encoded in a string
    val schemaStringBusiness = "business_id::full_address::categories"

    // Generate the schema of business based on the string of schema
    val fieldsBusiness = schemaStringBusiness.split("::")
                        .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schemaBusiness = StructType(fieldsBusiness)

    // Convert records of the RDD (business) to Rows
    val rowRDDBusiness = businessRDD.map(_.split("::")).map(items => Row(items(0),items(1),items(2)))

    // Apply the schema of business to the row RDD, to create DataFrame
    val businessDF = spark.createDataFrame(rowRDDBusiness,schemaBusiness)

    // Creates a temporary view using the DataFrame
    businessDF.createOrReplaceTempView("businessTable")

    /**** work on the review.csv ****/
    // Create an RDD of review
    val reviewRDD = spark.sparkContext.textFile(reviewFilePath)

    // The schema of review is encoded in a string
    val schemaStringReview = "review_id::user_id::business_id::rating"

    // Generate the schema of review based on the string of schema
    val fieldsReview = schemaStringReview.split("::")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schemaReview = StructType(fieldsReview)

    // Convert records of the RDD (review) to Rows
    val rowRDDReview = reviewRDD.map(_.split("::")).map(items => Row(items(0),items(1),items(2),items(3)))

    // Apply the schema of review to the row RDD, to create DataFrame
    val reviewDF = spark.createDataFrame(rowRDDReview,schemaReview)

    // Creates a temporary view using the DataFrame
    reviewDF.createOrReplaceTempView("reviewTable")

    // SQL can be run over a temporary view created using DataFrames
    val query = "SELECT user_id, rating " +
                "FROM businessTable, reviewTable " +
                "WHERE businessTable.business_id = reviewTable.business_id AND full_address LIKE '%Stanford%'"

    // The results of SQL queries are DataFrames and support all the normal RDD operations
    val results = spark.sql(query)

    // get the total number of lines
    val linesNum = results.count().toInt
    results.show(linesNum, false)
  }

}
