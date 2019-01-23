import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MyUDFTest{

  def main(args: Array[String]): Unit = {

    val sparkConfig = new SparkConf().setAppName("MyUDFTest").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()
    val sparkContext = spark.sparkContext
    val data = spark.read.json("E:\\bigdata\\大数据\\13大数据技术之Spark-1128~1213\\5.software\\spark-2.1.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
    data.createOrReplaceTempView("people")
    spark.udf.register("getName",  (name:String)=>"woc_"+name)

    spark.sql("select getName(name) from people").show()


    spark.close()

  }
}
