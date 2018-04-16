package net.sansa_stack.template.spark.graphOps

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.Iterator

object AllpathsImpovised {

  class Path(val srcId: VertexId, val edgeLabel: String, val dstId: VertexId) extends Serializable {

    def edgeToString(): String = {

      val stringRep = srcId + " ; " +
        edgeLabel + " ; " + dstId
      return stringRep
    }
    def containsId(vid: Long): Boolean = {
      return (srcId == vid || dstId == vid)
    }
  }

  def runPregel[VD, ED: ClassTag](
    graph: Graph[VD, ED], activeDirection: EdgeDirection): VertexRDD[List[List[Path]]] = {

    val initialMsg = List.empty[List[Path]]
    val pregelGraph = graph.mapVertices((id, nodeData) => (id, List.empty[List[Path]])).cache
    val messages = pregelGraph.pregel[List[List[Path]]](initialMsg, 3, activeDirection)(
      //Pregel Vertex program
      (vid, vertexDataWithPaths, newPathsReceived) => {
        // Always save path
        (vertexDataWithPaths._1, vertexDataWithPaths._2 ++ newPathsReceived)

      },
      //Pregel Send Message
      triplet => {

        val srcVertexPath = triplet.srcAttr._2
        val receivedPathsDest = triplet.dstAttr._2
        val pathLength = getPathLength(srcVertexPath, receivedPathsDest)

        if (pathLength == 0) {
          val path = new Path(triplet.srcId, triplet.attr.toString(), triplet.dstId)
          Iterator((triplet.dstId, List(List(path))))
        } else {
          var sendMsgIterator: Set[(VertexId, List[List[Path]])] = Set.empty

          if (isNodeActive(triplet.srcId, srcVertexPath, pathLength)) {

            val filteredPathsSrc = srcVertexPath.filter(path => path.exists(edge => !edge.containsId(triplet.dstId)))
            if (filteredPathsSrc.length != 0) {
              val newEdgeToAddToPathsSrc = new Path(triplet.srcId, triplet.attr.toString(),
                triplet.dstId)
              val newPathsSrc = filteredPathsSrc.map(path => newEdgeToAddToPathsSrc :: path)
              val sendMsgDest = (triplet.dstId, newPathsSrc)
              sendMsgIterator = sendMsgIterator.+(sendMsgDest)
            }
          }
          if (isNodeActive(triplet.dstId, receivedPathsDest, pathLength)) {
            val filteredPathsDest = receivedPathsDest.filter(path => path.exists(edge => !edge.containsId(triplet.srcId)))
            if (filteredPathsDest.length != 0) {
              val newEdgeToAddToPathsDest = new Path(triplet.dstId, triplet.attr.toString(), triplet.srcId)
              val newPathsDst = filteredPathsDest.map(path => newEdgeToAddToPathsDest :: path)
              val sendMsgSrc = (triplet.srcId, newPathsDst)
              sendMsgIterator = sendMsgIterator.+(sendMsgSrc)
            }
          }
          sendMsgIterator.toIterator
        }
      },

      //Pregel Merge message
      (pathList1, pathList2) => pathList1 ++ pathList2) // End of Pregel

    val vertexWithType = messages.vertices
    //vertexWithType.collect().foreach(a => println(a._1 + " :" + a._2._2.foreach { x => for (a <- x) { a.edgeToString() } }))

    graph.vertices.innerJoin(vertexWithType)((vid, label, typeList) => (typeList._2))

  }

  def getPathLength(pathListsSrc: List[List[Path]], pathListDest: List[List[Path]]): Int = {

    if (pathListsSrc.length == 0 && pathListDest.length == 0)
      return 0
    else {
      val numPaths1 = pathListsSrc.length
      val numPaths2 = pathListDest.length
      if (numPaths1 == 0) {
        return pathListDest.head.size
      } else if (numPaths2 == 0) {
        return pathListsSrc.head.size
      } else {
        // Both src and destination have path information
        return Math.max(pathListsSrc.last.size, pathListDest.last.size)
      }
    }
  }

  def isNodeActive(nodeId: VertexId, pathList: List[List[Path]], iteration: Int): Boolean = {
    if (pathList.length == 0) {
      return false
    } else {
      return (pathList.head.size == iteration)
    }
  }
}
