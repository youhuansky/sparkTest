package RDDQueue

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object RDDQueueTest {


  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName("RDDQueue").set("spark.streaming.stopGracefullyOnShutdown","true").setMaster("local[*]")

    val ssc = new StreamingContext(sc, Seconds(2))
    //创建RDD队列
    val rddQueue = new mutable.SynchronizedQueue[RDD[Int]]()
    val inputStream = ssc.queueStream(rddQueue)
    //处理队列中的RDD数据
    val mappedStream = inputStream.map(x => (x % 10, 1))
    val reducedStream = mappedStream.reduceByKey(_ + _)

    reducedStream.print()
    ssc.start()
    for(i <-1 to 30){
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)

      //通过程序停止StreamingContext的运行
      //ssc.stop()

    }

  }

}
