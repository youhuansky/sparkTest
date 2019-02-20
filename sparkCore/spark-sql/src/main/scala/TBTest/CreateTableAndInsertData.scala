package TBTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CreateTableAndInsertData {


  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf().setAppName("insertData").setMaster("spark://hadoop102:7077")
    val spark = SparkSession.builder().config(sparkConfig).config("spark.authenticate", false) .getOrCreate()
    val sparkContext = spark.sparkContext
    val tbStock = sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\tbStock.txt")
    import spark.implicits._
//    val ds = tbStock.map(a=>  {val strings = a.split(",");TBStock(strings(0),strings(1),strings(2))}).toDS()
//    ds.createOrReplaceTempView("tbStock")
    val ds = tbStock.map(a=> a.split(",")).map(strs => (strs(0),strs(1),strs(2))).toDF("ordernumber","locationid","dateid")
    ds.createOrReplaceTempView("tbStock")
    spark.sql("select * from tbStock").write.mode("append").saveAsTable("tbStock")

    val tbStockDetail = sparkContext.textFile("/opt/spark/mytxt/tbStockDetail.txt")
    val ds2 = tbStockDetail.map(a=> a.split(",")).map(attr => (attr(0),attr(1).trim().toInt,attr(2),attr(3).trim().toInt,attr(4).trim().toDouble, attr(5).trim().toDouble)).toDF("ordernumber","rownum","itemid","number","price","amount")
    ds2.createOrReplaceTempView("tbStockDetail")
    spark.sql("select * from tbStockDetail").write.mode("append").saveAsTable("tbStockDetail")


    val tbDate = sparkContext.textFile("/opt/spark/mytxt/tbDate.txt")
    val ds3 = tbDate.map(a=> a.split(",")).map(attr => (attr(0),attr(1).trim().toInt, attr(2).trim().toInt,attr(3).trim().toInt, attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt, attr(7).trim().toInt, attr(8).trim().toInt, attr(9).trim().toInt)).toDF("dateid","years","theyear","month","day","weekday","week","quarter","period","halfmonth")
    ds3.createOrReplaceTempView("tbDate")
    spark.sql("select * from tbDate").write.mode("append").saveAsTable("tbDate")


    //计算所有订单中每年的销售单数、销售总额
    spark.sql("select c.theyear as year,count(distinct a.ordernumber) num,sum(b.amount) from tbStock a join tbStockDetail b on a.ordernumber =b.ordernumber join tbDate c on a.dateid =c.dateid group by c.theyear ").show

    //计算所有订单每年最大金额订单的销售额
    spark.sql("select tmp.year year,max(tmp.amount) maxamount from (select c.theyear year,b.ordernumber ordernum ,sum(b.amount) amount from tbStock a join tbStockDetail b on a.ordernumber =b.ordernumber join tbDate c on a.dateid =c.dateid group by c.theyear, b.ordernumber) tmp group by year order by year").show

    //计算所有订单中每年最畅销货品
    spark.sql("select  b.itemid itemid ,sum(b.amount) amount,c.theyear theyear from tbStock a join tbStockDetail b on a.ordernumber =b.ordernumber join tbDate c on a.dateid =c.dateid group by itemid,theyear").show
    spark.stop()








  }


}
