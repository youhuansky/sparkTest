import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

object MyUDAFTest extends UserDefinedAggregateFunction{
  //输入数据的类型
  override def inputSchema: StructType =StructType(StructField("age",IntegerType)::Nil)
  //数据存储区的类型
  override def bufferSchema: StructType = StructType(StructField("age",IntegerType)::StructField("count",IntegerType)::Nil)
  //输出数据的类型
  override def dataType: DataType = IntegerType
  //是否利用缓存，如果输入一样的话，结果一样
  override def deterministic: Boolean = true
  //初始化工作
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0)=0
      buffer(1)=0

  }
  //每个分区的每条数据循环的工作
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0)=buffer.getInt(0)+input.getInt(0)
    buffer(1)=buffer.getInt(1)+1

  }
  //对不同分区的合并操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0)=buffer1.getInt(0)+buffer2.getInt(0)
    buffer1(1)=buffer1.getInt(1)+buffer2.getInt(1)


  }
  //计算
  override def evaluate(buffer: Row): Any = {
    return buffer.getInt(0)/ buffer.getInt(1)


  }


  def main(args: Array[String]): Unit = {
    val sparkConfig = new SparkConf().setAppName("MyUDFTest").setMaster("local[*]")
    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()
    val sparkContext = spark.sparkContext
    val data = spark.read.json("E:\\bigdata\\大数据\\13大数据技术之Spark-1128~1213\\5.software\\spark-2.1.1-bin-hadoop2.7\\examples\\src\\main\\resources\\people.json")
    data.createOrReplaceTempView("people")
    spark.udf.register("avgAge",MyUDAFTest)
    spark.sql("select avgAge(age) from people").show()
  }
}
