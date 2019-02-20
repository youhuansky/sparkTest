package KafkaStreaming

import java.util.Properties

import org.apache.commons.pool2.BasePooledObjectFactory
import org.apache.commons.pool2.impl.{DefaultPooledObject, GenericObjectPool}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

//用于通过池化方式获取连接kafka的对象，然后利用对象将数据写入到kafka

//创建一个kafkaproducer的代理类
class KafkaProxy(brokenList: String, topic: String) {

  val producer = {
    //创建一个配置属性
    val property = new Properties()
    property.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokenList)
    property.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    property.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](property)
  }

  //封装的send消息的方法
  def send(key:String,value:String): Unit ={
    producer.send(new ProducerRecord[String,String](topic,key,value))
  }

}
//这个类是创建池化对象的工厂类
class KafkaProxyFactory(brokenList:String,topic:String) extends   BasePooledObjectFactory[KafkaProxy]{
  //创建实体
  override def create() = new KafkaProxy(brokenList,topic)

  //创建实体
  override def wrap(t: KafkaProxy) = new DefaultPooledObject[KafkaProxy](t)

}

//执行具体的创建
object KafkaProxyPool{

  private var pool:GenericObjectPool[KafkaProxy]=null
  //获取池对象
  def apply(brokenList:String,topic:String) :GenericObjectPool[KafkaProxy]={

    if(null==pool){

      val factory = new KafkaProxyFactory(brokenList,topic)

      pool=new GenericObjectPool[KafkaProxy](factory)
    }
    return pool
  }
//可以有业务逻辑
//  pool.borrowObject()
//  pool.returnObject()
}