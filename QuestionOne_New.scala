import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by monkeyzxr on 2017/4/1.
  */
object QuestionOne_New {
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

    //get key-value line by tuple(business_id, full_address)
    val businessKV = businessLine.map(line => (line.split("::")(0), line.split("::")(1)))
    val businessKV_Filter = businessKV.filter(tuple => tuple._2.contains("Stanford"))
    //businessKV_Filter.collect().foreach(println)

    //get key-value line by tuple(business_id, (user_id, stars))
    val reviewKV = reviewLine.map(line => (line.split("::")(2), (line.split("::")(1), line.split("::")(3))))

    val joinData = businessKV_Filter.join(reviewKV)

    //joinData.collect().foreach(println), result likes:
    //(BC5Xm5HfcA4BdB91cFQm4g,(459 Lagunita DrStanford, CA 94305,(ybkYMP-SjJB8Sn-bnbYluQ,4.0)))
    //(BC5Xm5HfcA4BdB91cFQm4g,(459 Lagunita DrStanford, CA 94305,(e6QRXwwzhebaLVX0JEztpg,3.0)))
    //(BC5Xm5HfcA4BdB91cFQm4g,(459 Lagunita DrStanford, CA 94305,(uv3XzRc6rT6r_Xd-7aRIzA,4.0)))
    val result = joinData.map(line => line._2._2)

    result.collect().foreach(println)
  }

}
