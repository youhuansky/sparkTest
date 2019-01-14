package myPartitioner

import org.apache.spark.{Partitioner, SparkConf, SparkContext}


class MyPartitioner(num: Int) extends Partitioner {

  override def numPartitions = num

  override def getPartition(key: Any): Int = {

    val string = key.toString
    val strings = string.split("\\.")


    return strings(1).toInt % num

  }

}


object mainObj extends App {

  override def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("wordCountTest").setMaster("local[*]")
    //2.创建一个sparkcontext用于连接spark集群
    val sc = new SparkContext(sparkConf)
    //3.加载需要处理的数据文本
    val rdd = sc.makeRDD(List("aa.2","bb.2","cc.3","dd.3","ee.5"))
    val rdd2 = rdd.map((_,1)).partitionBy(new MyPartitioner(5))

    rdd2.mapPartitionsWithIndex((index,items)=>Iterator(index +" : "+items.mkString("|"))).foreach(println)

  }


}