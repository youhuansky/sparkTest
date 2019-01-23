import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StructType}

object MyUDAFTest extends UserDefinedAggregateFunction{
  //输入数据的类型
  override def inputSchema: StructType = ???
  //数据存储区的类型
  override def bufferSchema: StructType = ???
  //输出数据的类型
  override def dataType: DataType = ???
  //是否利用缓存，如果输入一样的话，结果一样
  override def deterministic: Boolean = ???
  //初始化工作
  override def initialize(buffer: MutableAggregationBuffer): Unit = ???
  //每个分区的每条数据循环的工作
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = ???
  //对不同分区的合并操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = ???
  //计算
  override def evaluate(buffer: Row): Any = ???
}
