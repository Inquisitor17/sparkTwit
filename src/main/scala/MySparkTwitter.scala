import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._
import org.apache.spark.{ SparkContext, SparkConf }
import java.io.File

object MySparkTwitter {
  def main(args: Array[String]){
    if (args.length < 4){
     System.err.println("<consumer key> <secrets> <access token> <access token secret>")
     System.exit(1)
    }
  
  val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
  val filters = args.takeRight(args.length -4)
  
  System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
  System.setProperty("twitter4j.oauth.accessToken", accessToken)
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

  val sparkConf = new SparkConf().setAppName("twitterSentiment").setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  val stream = TwitterUtils.createStream(ssc, None, filters)
  
  val hashTags = stream.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
  
  val topCountMinute = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
    .map {case (topic, count) => (count, topic)}
    .transform(_.sortByKey(false))
    
   val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
    .map {case (topic, count) => (count, topic)}
    .transform(_.sortByKey(false))
    
   stream.print()
   
   topCountMinute.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })
    topCounts10.foreachRDD(rdd => {
      val topList = rdd.take(10)
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach { case (count, tag) => println("%s (%s tweets)".format(tag, count)) }
    })
    
    ssc.start()
    ssc.awaitTermination()
  }
}
  
