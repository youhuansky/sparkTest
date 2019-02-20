package KafkaStreaming

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaTest {


  def main(args: Array[String]): Unit = {

    val sc = new SparkConf().setAppName("kafkaTest").setMaster("local[*]")

    val ssc = new StreamingContext(sc, Seconds(2))

    //连接kafka
    val kafkaParams = Map (
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092,hadoop103:9092,hadoop104:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "kafka",
      //earliest,latest,比如在有新的CG加入，那么smallest表示从最小的offset开始消费，lastest是从当前tocpic最
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> (true: java.lang.Boolean)
    )

    val lines = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](Iterable("log"), kafkaParams))

    //执行逻辑
    lines.map(str => "......." + str.value()).foreachRDD(
      rdd => {
        rdd.foreachPartition(items => {
          //获取kafka连接池
          val pool = KafkaProxyPool("hadoop102:9092,hadoop103:9092,hadoop104:9092", "log2")
          val proxy = pool.borrowObject()
          for(item<-items){
            println(item)
            proxy.send("kafka", item)
          }
          pool.returnObject(proxy)
        }
        )
      }
    )
    //启动程序
    ssc.start()

    ssc.awaitTermination()
  }

}
