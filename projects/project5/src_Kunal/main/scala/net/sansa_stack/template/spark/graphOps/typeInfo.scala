package net.sansa_stack.template.spark.graphOps
import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.Iterator

object typeInfo {

  def getTypeInfo(graph: Graph[String, String]): VertexRDD[List[String]] = {
    val initialMsg: List[String] = List.empty
    val maxIterations = 5
    val activeDirection = EdgeDirection.Out

    
    val pregelGraph = graph.mapVertices((id, label) => (id, label, List.empty[String]))
    val messages = pregelGraph.pregel[List[String]](initialMsg, maxIterations, activeDirection)(
      (vid, oldMessage, newMessage) => (vid, oldMessage._2, newMessage ++ oldMessage._3),

      (triplet) => {
        if (triplet.attr.toString().contains("type"))

          Iterator((triplet.srcId, List(triplet.dstAttr._2)))
        else
          Iterator.empty
      },
      (msg1, msg2) => msg1 ++ msg2)

      
      
    val vertexWithType = messages.vertices
    graph.vertices.innerJoin(vertexWithType)((vid, label, typeList) => (typeList._3))

  }

}
