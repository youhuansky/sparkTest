import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkSql {

  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf().setAppName("spark-sql").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()
    val sparkContext = spark.sparkContext
    val data = spark.read.json("E:\\bigdata\\大数据\\13大数据技术之Spark-1128~1213\\5.software\\spark-2.1.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
    val rdd = sparkContext.textFile("E:\\bigdata\\大数据\\13大数据技术之Spark-1128~1213\\5.software\\spark-2.1.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.txt")
    // For implicit conversions like converting RDDs to DataFrames
    // 通过引入隐式转换可以将RDD的操作添加到 DataFrame上。
    import spark.implicits._
//    data.filter($"age" > 22).show()
//    data.select($"name").show()
    val ds = data.as("people")
    data.createOrReplaceTempView("people2")
    spark.sql("select * from people2 where name ='Andy'").show()
    val df2 = rdd.map(line => line.split(",")).map(strs => (strs(0),strs(1).trim.toInt)).toDF("name","age")
    df2.show()
  }


}
