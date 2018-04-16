package net.sansa_stack.template.spark.graphOps
import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.Iterator
object Allpaths {
  class Path(val srcId: VertexId, val edgeLabel: String, val dstId: VertexId,
             val isOutgoing: Boolean = true) extends Serializable {

    def edgeToString(): String = {
      var directionString = "Outbound"
      if (isOutgoing == false)
        directionString = "Inbound"
      val stringRep = directionString + " ; " + srcId + " ; " +
        edgeLabel + " ; " + dstId
      return stringRep
    }

    def containsId(vid: Long): Boolean = {
      return (srcId == vid || dstId == vid)
    }
  }

  def runPregel[VD, ED: ClassTag](src: (VertexId), dest: (VertexId),
                                  graph: Graph[VD, ED], activeDirection: EdgeDirection): List[List[Path]] = {

    val pathSrcId = src
    val pathDstId = dest
    val initialMsg = List.empty[List[Path]]
    val pregelGraph = graph.mapVertices((id, nodeData) => (id, List.empty[List[Path]])).cache
    val messages = pregelGraph.pregel[List[List[Path]]](initialMsg, 3, activeDirection)(
    
        //Pregel Vertex program
      (vid, vertexDataWithPaths, newPathsReceived) => {
        // Save paths only if destination found.
        if (vid == pathDstId)
          (vertexDataWithPaths._1, vertexDataWithPaths._2 ++ newPathsReceived)
        else
          (vertexDataWithPaths._1, newPathsReceived)
      },

      //Pregel Send Message 
      triplet => {

        val receivedPathsSrc = triplet.srcAttr._2
        val receivedPathsDest = triplet.dstAttr._2
        val pathLength = getPathLength(receivedPathsSrc, receivedPathsDest)

        if (pathLength == 0) {
          if (triplet.srcId == pathSrcId && (triplet.dstId == pathDstId)) {
            val path = new Path(triplet.srcId, triplet.attr.toString(), triplet.dstId, true)
            Iterator((triplet.dstId, List(List(path))))
          } else if (triplet.dstId == pathSrcId && (triplet.srcId == pathDstId)) {
            val path = new Path(triplet.srcId, triplet.attr.toString(), triplet.dstId, true)
            Iterator((triplet.srcId, List(List(path))))
          } else {
            Iterator.empty
          }
        } else {
          var sendMsgIterator: Set[(VertexId, List[List[Path]])] = Set.empty

          if (isNodeActive(triplet.srcId, receivedPathsSrc, pathLength, pathDstId) &&
            (triplet.dstId == pathDstId)) {

            val filteredPathsSrc = receivedPathsSrc.filter(path => path.exists(edge => !edge.containsId(triplet.dstId)))
            if (filteredPathsSrc.length != 0) {
              val newEdgeToAddToPathsSrc = new Path(triplet.srcId, triplet.attr.toString(),
                triplet.dstId, true)
              val newPathsSrc = filteredPathsSrc.map(path => newEdgeToAddToPathsSrc :: path)
              val sendMsgDest = (triplet.dstId, newPathsSrc)
              sendMsgIterator = sendMsgIterator.+(sendMsgDest)
            }
          }

          if (isNodeActive(triplet.dstId, receivedPathsDest, pathLength, pathDstId) &&
            (triplet.srcId == pathDstId)) {

            val filteredPathsDest = receivedPathsDest.filter(path => path.exists(edge => !edge.containsId(triplet.srcId)))
            if (filteredPathsDest.length != 0) {
              val newEdgeToAddToPathsDest = new Path(triplet.dstId,
                triplet.attr.toString(), triplet.srcId, false)

              //Append new edge to remaining and send
              val newPathsDst = filteredPathsDest.map(path => newEdgeToAddToPathsDest :: path)
              val sendMsgSrc = (triplet.srcId, newPathsDst)
              sendMsgIterator = sendMsgIterator.+(sendMsgSrc)
            }
          }
          sendMsgIterator.toIterator
        }
      },

      //Merge paths in case of multiple paths 
      (pathListSrc, pathListfromOtherSrc) => pathListSrc ++ pathListfromOtherSrc)

    val allPathsToDestination = messages.vertices.filter(_._1 == pathDstId).collect.apply(0)._2._2
    return allPathsToDestination
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

  def isNodeActive(nodeId: VertexId, pathList: List[List[Path]], iteration: Int, finalDestId: VertexId): Boolean = {
    if (nodeId == finalDestId || pathList.length == 0) {
      return false
    } else {
      return (pathList.head.size == iteration)
    }
  }
}