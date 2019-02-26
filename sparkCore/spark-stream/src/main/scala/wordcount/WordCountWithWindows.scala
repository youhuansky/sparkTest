package wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountWithWindows {

//  窗口概念包含窗口大小和滑动步长，窗口大小指的一次取值包含多大范围，滑动步长表示多长时间取一次

  def main(args: Array[String]): Unit = {

    val sc = new SparkConf().setAppName("WordCountWithWindows").setMaster("local[*]")
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.sparkContext.setCheckpointDir("C:\\Users\\Administrator\\Desktop\\checkPoint")
    val sts = ssc.socketTextStream("hadoop102",9999)
    val wordRes = sts.flatMap(_.split(" ")).map((_,1))

//    val wordWinRes = wordRes.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b), Seconds(15),Seconds(10))
    //这是优化后的算法，使用增量计算，第二个函数表示对过期的窗口RDD采用的去除策略
    val wordWinRes = wordRes.reduceByKeyAndWindow((a:Int,b:Int)=>(a+b),(a:Int,b:Int)=>(a-b), Seconds(15),Seconds(10))
    println("+++++++++++++++++++++++++++++++++++++++++++++++++++++")
    wordWinRes.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
