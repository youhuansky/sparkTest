import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphXHelloWorld {


  def main(args: Array[String]): Unit = {

    //1.获取sparkconf连接
    val sparkConf = new SparkConf().setAppName("graphXHelloWorld").setMaster("local[*]")
    //2.创建一个sparkcontext
    val sc = new SparkContext(sparkConf)
    //3.创建顶点RDD
    val vertexRDD: RDD[(VertexId, (String, String))] = sc.makeRDD(Array((3L, ("rxin", "stu")), (5L, ("franklin", "prof")), (7L, ("jgonzal", "pst.doc")), (2L, ("istoica", "prof"))))
    //4.创建边的RDD
    val edgeRdd: RDD[Edge[String]] = sc.makeRDD(Array(Edge(5L, 3L, "Advisor"), Edge(3L, 7L, "Collab"), Edge(5L, 7L, "PI"), Edge(2L, 5L, "Cokkeague")))
    //5.(1)常规创建图
        val graphx = Graph(vertexRDD,edgeRdd)
    // (2)从边构造图
    //    val graphx = Graph.fromEdges(edgeRdd,"aa")
    // (3)从边的元组构造图
    //    val rawEdge = edgeRdd.map(item => (item.srcId, item.dstId))
    //    val graphx = Graph.fromEdgeTuples(rawE
    //
    //
    //
    // dge, "aa")

    val subVertexRDD = graphx.vertices.filter{case (id,(name ,job))=>job=="pst.doc"}
    val subGraphx = Graph(subVertexRDD,edgeRdd)
    //6.输出图结果
//    graphx.triplets.collect().foreach(item => println("源顶点：" + item.srcId + " [" + item.srcAttr + "] -> [" + item.attr + "] -> 目标顶点：" + item.dstId + " [" + item.dstAttr + "]"))
    subGraphx.triplets.collect().foreach(item => println("源顶点：" + item.srcId + " [" + item.srcAttr + "] -> [" + item.attr + "] -> 目标顶点：" + item.dstId + " [" + item.dstAttr + "]"))
    //7.关闭连接
    sc.stop()
  }


}
