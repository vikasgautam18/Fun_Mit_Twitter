package com.wordpress.technicado.longest_tweet

import java.util.concurrent.atomic.AtomicLong

import com.wordpress.technicado.commons.Utils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println(s"USAGE: spark-submit --class ${this.getClass.getCanonicalName} " +
        "--master local[*] spark/jars/fun-mit-twitter_2.11-0.1.jar hdfs://path/to/twitter.config")
      System.exit(-1)
    }

    val conf = new SparkConf().setAppName("averagetweetlength")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(2))
    Utils.setUpTwitter(args(0), ssc.sparkContext)

    val status = Utils.createTwitterStream(ssc)
      .mapPartitions(iter => iter.map(_.getText))

    val counter = ssc.sparkContext.longAccumulator("tweetCount")
    var totalChars = new AtomicLong(0)

    Utils.enableErrorLoggingOnly

    status.foreachRDD(rdd => {
      if(rdd.count > 0){
        counter.add(rdd.count)
        val tLen = rdd.map(_.length).max
        if(totalChars.get() < tLen) totalChars.set(tLen)

        println(s"**** total tweets = ${counter.value} **** longest tweet = ${totalChars.get} chars ****")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
