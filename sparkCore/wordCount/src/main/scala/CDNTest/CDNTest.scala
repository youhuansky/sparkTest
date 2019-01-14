package CDNTest

import org.apache.spark.{SparkConf, SparkContext}

object CDNTest {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("CDNTest").setMaster("local[*]")
    //2.创建一个sparkcontext用于连接spark集群
    val sc = new SparkContext(sparkConf)
    //3.加载需要处理的数据文本
    val rdd = sc.textFile("C:\\Users\\Administrator\\Desktop\\cdn.txt")
    rdd.map(line => (line.split(" ")(0), 1)).reduceByKey(_ + _).sortBy(a => a._2, false).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\r1")
    rdd.map(line => line.split(" ")(6) + "_" + line.split(" ")(0)).filter(a => a.contains("mp4")).map(a => (a.split("_")(0), a.split("_")(1))).distinct().groupByKey().map(a => (a._1, a._2.size)).sortBy(a => a._2, false).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\r2")
    rdd.map(line => getTime(line)).reduceByKey(_ + _).map(a => (a._1, a._2 / 1024 / 1024 / 1024)).sortBy(a => a._2, false).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\r3")
    sc.stop()

  }


  def getTime(str: String): (String, Double) = {

    val strings = str.split(" ")
    val v1 = strings(3).split(":")(1)
    val v2 = strings(9)
    return (v1, v2.toDouble)
  }
}
