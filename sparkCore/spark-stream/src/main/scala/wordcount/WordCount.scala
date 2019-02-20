package wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCount {


  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName("streamming").set("spark.streaming.stopGracefullyOnShutdown","true").setMaster("local[*]")

    val ssc = new StreamingContext(sc, Seconds(2))
    val sts = ssc.socketTextStream("127.0.0.1",9999)
    sts.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()

  }
}
