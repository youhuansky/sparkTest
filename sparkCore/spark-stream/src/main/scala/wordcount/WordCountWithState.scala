package wordcount

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountWithState {


  def main(args: Array[String]): Unit = {
    val sc = new SparkConf().setAppName("WordCountWithState").setMaster("local[*]")
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.sparkContext.setCheckpointDir("C:\\Users\\Administrator\\Desktop\\checkPoint")
    val sts = ssc.socketTextStream("hadoop102",9999)
    val wordRes = sts.flatMap(_.split(" ")).map((_,1))
    //    wordRes.print()
    //    println("+++++++++++++++++++++++++++++++++++++++++")
    val wordStateRes=wordRes.updateStateByKey[Int]((word:Seq[Int],count:Option[Int])=>{
      //      println("sum is "+word.mkString(","))
      //      println("word is "+count)
      Some(word.sum+count.getOrElse(0))
    })
    wordStateRes.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
