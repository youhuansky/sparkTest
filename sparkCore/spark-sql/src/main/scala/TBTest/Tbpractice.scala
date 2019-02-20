package TBTest

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
//练习的另一个版本
object Tbpractice {
  // case class 交易
  case class tbStock(ordernumber:String,locationid:String,dateid:String) extends Serializable

  //
  case class tbStockDetail(ordernumber:String, rownum:Int, itemid:String, number:Int, price:Double, amount:Double) extends Serializable

  case class tbDate(dateid:String, years:Int, theyear:Int, month:Int, day:Int, weekday:Int, week:Int, quarter:Int, period:Int, halfmonth:Int) extends Serializable
  private def insertHive(spark:SparkSession,table:String,data:DataFrame): Unit ={
    spark.sql("drop table if exists "+ table)
    data.write.saveAsTable(table)
  }

  private def insertMySQL(table:String,data:DataFrame): Unit ={
    data.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark")
      .option("dbtable", table)
      .option("user", "root")
      .option("password", "root")
      .save()
  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("Practice").setMaster("local[*]")
      .set("spark.sql.warehouse.dir","hdfs://master01:9000/spark_warehouse2")

    val spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    import spark.implicits._
    //将数据加载到Hive
    val tbStockRDD = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\spark\\sparksql\\practice\\src\\main\\resources\\tbStock.txt")
    val tbStockDS = tbStockRDD.map(_.split(",")).map(attr=>tbStock(attr(0),attr(1),attr(2))).toDS()
    insertHive(spark,"tbStock",tbStockDS.toDF)

    val tbStockDetailRDD = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\spark\\sparksql\\practice\\src\\main\\resources\\tbStockDetail.txt")
    val tbStockDetailDS = tbStockDetailRDD.map(_.split(",")).map(attr=> tbStockDetail(attr(0),attr(1).trim().toInt,attr(2),attr(3).trim().toInt,attr(4).trim().toDouble, attr(5).trim().toDouble)).toDS
    insertHive(spark,"tbStockDetail",tbStockDetailDS.toDF)

    val tbDateRDD = spark.sparkContext.textFile("C:\\Users\\Administrator\\Desktop\\spark\\sparksql\\practice\\src\\main\\resources\\tbDate.txt")
    val tbDateDS = tbDateRDD.map(_.split(",")).map(attr=> tbDate(attr(0),attr(1).trim().toInt, attr(2).trim().toInt,attr(3).trim().toInt, attr(4).trim().toInt, attr(5).trim().toInt, attr(6).trim().toInt, attr(7).trim().toInt, attr(8).trim().toInt, attr(9).trim().toInt)).toDS
    insertHive(spark,"tbDate",tbDateDS.toDF)

    //需求一：
    val result1 = spark.sql("SELECT c.theyear, COUNT(DISTINCT a.ordernumber), SUM(b.amount) FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear ORDER BY c.theyear")
    insertMySQL("xq1",result1)

    //需求二：
    val result2 = spark.sql("SELECT theyear, MAX(c.SumOfAmount) AS SumOfAmount FROM (SELECT a.dateid, a.ordernumber, SUM(b.amount) AS SumOfAmount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber GROUP BY a.dateid, a.ordernumber ) c JOIN tbDate d ON c.dateid = d.dateid GROUP BY theyear ORDER BY theyear DESC")
    insertMySQL("xq2",result2)

    //需求三：
    val result3 = spark.sql("SELECT DISTINCT e.theyear, e.itemid, f.maxofamount FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS sumofamount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) e JOIN (SELECT d.theyear, MAX(d.sumofamount) AS maxofamount FROM (SELECT c.theyear, b.itemid, SUM(b.amount) AS sumofamount FROM tbStock a JOIN tbStockDetail b ON a.ordernumber = b.ordernumber JOIN tbDate c ON a.dateid = c.dateid GROUP BY c.theyear, b.itemid ) d GROUP BY d.theyear ) f ON e.theyear = f.theyear AND e.sumofamount = f.maxofamount ORDER BY e.theyear")
    insertMySQL("xq3",result3)

    spark.close()

  }

}
