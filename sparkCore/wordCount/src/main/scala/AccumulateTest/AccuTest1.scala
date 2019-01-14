package AccumulateTest

import org.apache.spark.{SparkConf, SparkContext}

object AccuTest1 {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("accu1Test").setMaster("local[*]")
    //2.创建一个sparkcontext用于连接spark集群
    val sc = new SparkContext(sparkConf)
    //3.加载需要处理的数据文本
    val rdd = sc.textFile("C:\\Users\\Administrator\\Desktop\\NOTICE")
    val acc = sc.accumulator(0)
    val unit = rdd.map(line =>if(line==""){acc.add(1) }).foreach(println)
    println("总行数量是："+acc.value)


    sc.stop()

  }

}
