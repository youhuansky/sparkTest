package wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Appltcation extends App {

  //1.声明一个spark conf对象，用于配置spark连接
  val sparkConf = new SparkConf().setAppName("wordCountTest")/*.setMaster("local[*]")*/
  //2.创建一个sparkcontext用于连接spark集群
  val sparkContext = new SparkContext(sparkConf)
  //3.加载需要处理的数据文本
  val textFile = sparkContext.textFile("hdfs://hadoop102:9000/test/0223.txt")
  val value: RDD[String] = textFile.flatMap(_.split("\r\n"))
  val value1: RDD[(String, Int)] = value.map(getPartnerordeid(_)).map((_, 1)).reduceByKey(_ + _, 1)
  value1.saveAsTextFile("hdfs://hadoop102:9000/test/0223_result.txt")
  //4.输出数据文件的结果
  value1.saveAsTextFile("hdfs://hadoop102:9000/test/0223_result.txt")

  //5.关闭spark集群的连接
  sparkContext.stop()

  def getPartnerordeid(str: String): String = {
    println(str)
    var partnerordeid = str.split("\\|")(8)
    println(partnerordeid)
    return partnerordeid;
  }
}
