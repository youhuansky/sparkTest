package CustomReceiver

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket
import java.nio.charset.StandardCharsets

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.receiver.Receiver


class MyCustomReceiver(host:String,port:Int) extends  Receiver[String](StorageLevel.MEMORY_AND_DISK_2){
  override def onStart(): Unit = {

    // Start the thread that receives data over a connection
    new Thread("Socket Receiver") {
      override def run() { receive() }
    }.start()

  }

  override def onStop(): Unit = {


  }


  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    var socket: Socket = null
    var userInput: String = null
    try {
      // Connect to host:port
      socket = new Socket(host, port)

      // Until stopped or connection broken continue reading
      val reader = new BufferedReader(
        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8))
      userInput = reader.readLine()
      while(!isStopped && userInput != null) {
        store(userInput)
        userInput = reader.readLine()
      }
      reader.close()
      socket.close()

      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting to " + host + ":" + port, e)
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }
}


object  MyCustomReceiverTest{


  def main(args: Array[String]): Unit = {


    val sc = new SparkConf().setAppName("MyCustomReceiverTest").set("spark.streaming.stopGracefullyOnShutdown","true").setMaster("local[*]")

    val ssc = new StreamingContext(sc, Seconds(2))
    val customReceiverStream = ssc.receiverStream(new MyCustomReceiver("hadoop102", 9999))
    customReceiverStream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).print()
    ssc.start()
    ssc.awaitTermination()
  }

}