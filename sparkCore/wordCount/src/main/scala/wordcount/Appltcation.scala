package wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Appltcation extends App {

  //1.声明一个spark conf对象，用于配置spark连接

  val sparkConf = new SparkConf().setAppName("wordCountTest").setMaster("local[*]")
  //2.创建一个sparkcontext用于连接spark集群
  val sparkContext = new SparkContext(sparkConf)
  //3.加载需要处理的数据文本
  val textFile = sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\25.txt")
  val value: RDD[String] = textFile.flatMap(_.split("\r\n"))
  val value1: RDD[(String, Int)] = value.map(getPartnerordeid(_)).map((_, 1)).reduceByKey(_ + _, 1).sortBy(a =>a._2,false )
  //4.输出数据文件的结果
  value1.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\0223_3_res")

  //5.关闭spark集群的连接
  sparkContext.stop()

  def getPartnerordeid(str: String): String = {
    println(str)
    var partnerordeid = str.split("\\|",-1)(11)
    println(partnerordeid)
    return partnerordeid;
  }
}



object Appltcation2 extends App {

  //1.声明一个spark conf对象，用于配置spark连接

  val sparkConf = new SparkConf().setAppName("wordCountTest").setMaster("local[*]")
  //2.创建一个sparkcontext用于连接spark集群
  val sparkContext = new SparkContext(sparkConf)
  //3.加载需要处理的数据文本
  val textFile = sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\youhuan.log")
  textFile.filter(dofilter(_)).map(str=>(str.split(","))(1)).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\yh")

  /*val value: RDD[String] = textFile.flatMap(_.split("\r\n"))
    val value1: RDD[(String, Int)] = value.map(getPartnerordeid(_)).map((_, 1)).reduceByKey(_ + _, 1).sortBy(a =>a._2,false )
    //4.输出数据文件的结果
    value1.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\0223_3_res")*/
  //5.关闭spark集群的连接
  sparkContext.stop()

  def dofilter(str: String): Boolean = {
    if(str.contains("API")||str.contains("ALIVE")){
      return false
    }else{
      val strings = str.split(",")

      if("0110".equals(strings(8))||"0126".equals(strings(8))){
          return false
      }


      return true
    }
  }
}