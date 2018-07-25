package com.wordpress.technicado.savetweets

import com.wordpress.technicado.commons.{ConfigReader, Constants, Utils}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status

object Main {
  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("USAGE: spark-submit --class com.wordpress.technicado.savetweets.Main " +
        "--master local[*] spark/jars/fun-mit-twitter_2.11-0.1.jar hdfs://path/to/twitter.config")
      System.exit(-1)
    }

    val conf = new SparkConf().setAppName("savetweets")
    val ssc = new StreamingContext(conf, Seconds(2))

    Utils.setUpTwitter(args(0), ssc.sparkContext)

    val tweets: ReceiverInputDStream[Status] = Utils.createTwitterStream(ssc)

    val tweetText: DStream[String] = tweets.mapPartitions(iter => iter.map(_.getText))
    var countTweets: Long = 0L

    Utils.enableErrorLoggingOnly

    tweetText.foreachRDD((rdd, time) => {
      if(rdd.count > 0) {
        val newPartition: RDD[String] = rdd.repartition(1).cache()
        rdd.saveAsTextFile(ConfigReader.getString(Constants.TWITTER_STORAGE_PATH) + time.milliseconds)
        countTweets += newPartition.count
        println(s"***** total $countTweets tweets written")
        if (countTweets > ConfigReader.getLong(Constants.TWITTER_STATUS_LIMIT))
          System.exit(-1)
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
