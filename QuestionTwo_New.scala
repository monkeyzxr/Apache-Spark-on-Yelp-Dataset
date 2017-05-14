import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by monkeyzxr on 2017/4/1.
  */
object QuestionTwo_New {
  def main(args: Array[String]): Unit = {
    val businessFilePath = "file:///Users/monkeyzxr/Desktop/" +
      "CS 6350.001 - Big Data Management and Analytics/Assignment/" +
      "Homework-2-partB/dataset/business.csv"
    val reviewFilePath = "file:///Users/monkeyzxr/Desktop/" +
      "CS 6350.001 - Big Data Management and Analytics/Assignment/" +
      "Homework-2-partB/dataset/review.csv"

    val conf = new SparkConf().setAppName("QuestionOne_New").setMaster("local")
    val sc = new SparkContext(conf)

    // get RDD
    val businessLine = sc.textFile(businessFilePath)  //"business_id"::"full_address"::"categories"
    val reviewLine = sc.textFile(reviewFilePath)  //"review_id"::"user_id"::"business_id"::"stars"

    //get key-value line by tuple(business_id, (business_id, full_address, categories))
    val businessKV = businessLine.map(line => (line.split("::")(0), (line.split("::")(0), line.split("::")(1), line.split("::")(2))))
   // businessKV.collect().foreach(println)

    // make key-value paris in review.csv by tuple(business_id, stars)
    val reviewKV = reviewLine.map(line => (line.split("::")(2), line.split("::")(3).toDouble))
    val reviewKV_taged = reviewKV.mapValues(stars => (stars, 1))
   // reviewKV_taged.collect().foreach(println)
    // result looks like:
   // (UoFOARh82bBOOpvGugtbFg,(2.0,1))
   // (tIIof6-zqlgxImFNDEIj9g,(4.0,1))
   // (8Qe6g3Dv5NXN1Zq-egJk9w,(4.0,1))

    val reviewKV_taged_sum = reviewKV_taged.reduceByKey((tuple1,tuple2) => (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2))
    //reviewKV_taged_sum.collect().foreach(println)
    // result looks like :
    // (3-AcFdxdvov9tL1C_3eT0g,(28.0,8))
    // (yA-D2JsM279hh07O47Xf7g,(5.0,2))
    // (WgZfIyGZTEB6086UtzEF6w,(337.0,68))

    // calculate the avg of each tuple
    val reviewKV_avg = reviewKV_taged_sum.map(tuple => (tuple._1, tuple._2._1 / tuple._2._2))
   // reviewKV_avg.collect().foreach(println)
    // result looks like:
   // (aKzoUZv2iND_AFv62cuMYw,3.7777777777777777)
   // (gRiYNQeRRCQeHXHBp5m7PQ,4.6)
   // (9YsLJw8n4DBLsnchm-I8ZA,2.0)

    //implement Join and Distinct
    val joinData = businessKV.join(reviewKV_avg).distinct()

    //joinData.collect().foreach(println)
    // result looks like :
    //(gRiYNQeRRCQeHXHBp5m7PQ,((gRiYNQeRRCQeHXHBp5m7PQ,288 River StTroy, NY 12180,List(Women's Clothing, Fashion, Shopping)),4.6))

    val result_all = joinData.map(tuple => tuple._2)
    // result_all.collect().foreach(println)
    // result looks like:
    //((unE4t2dqs-05zXZmGxlawA,636 Beacon StBoston, MA 02215,List(Thai, Restaurants)),3.6226415094339623)

    val result_all_sorted = result_all.map(tuple => (tuple._2, tuple._1)).sortByKey(false).map(tuple => (tuple._2, tuple._1))

    val result_sorted_top10 = result_all_sorted.take(10)

    result_sorted_top10.foreach(println)
    //((RJYzt9Xwk0_PGcNow1B8lQ,2490 Telegraph AveTelegraph AveBerkeley, CA 94704,List(Arts & Crafts, Shopping, Jewelry, Antiques)),5.0)
    //((dgKeY0jJd3RZNVZeCgc1HA,3916 Locust WalkUniversity CityPhiladelphia, PA 19104,List(Churches, Religious Organizations)),5.0)


  }

}
