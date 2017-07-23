package net.sansa_stack.template.spark.graphOps

import scala.reflect.ClassTag
import org.apache.spark.graphx._
import scala.Iterator

// object tweeked to find shortest paths between all possible resources. Taken from SANSA shortest path (changed add maps method)  

object ShortestPaths {

  case class Path(length: Int, path: List[VertexId])
  type SPMap = Map[VertexId, Path]

  private def makeMap(x: (VertexId, Path)*) = Map(x: _*)

  private def incrementMap(dstId: VertexId, spmapDst: SPMap): SPMap = spmapDst map {
    case (vId, Path(length, path)) => (vId, Path(length + 1, dstId :: path))
  }

  private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap =
    (spmap1.keySet ++ spmap2.keySet).map { k =>
      k ->
        ((spmap1.get(k), spmap2.get(k)) match {
          case (Some(s1), Some(s2)) => if (s1.length < s2.length) s1 else s2
          case (Some(s1), _)        => s1
          case (_, Some(s2))        => s2
          case _                    => throw new RuntimeException("Nonsense")
        })
    }.toMap

  def run[VD, ED: ClassTag](graph: Graph[VD, ED], landmarks: List[VertexId]): Graph[SPMap, ED] = {
    val spGraph = graph.mapVertices { (vid, attr) =>
      if (landmarks.contains(vid)) makeMap(vid -> Path(0, Nil)) else makeMap()
    }

    val initialMessage = makeMap()

    def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
      addMaps(attr, msg)
    }

    def sendMessage(edge: EdgeTriplet[SPMap, _]): Iterator[(VertexId, SPMap)] = {
      val newAttr = incrementMap(edge.dstId, edge.dstAttr)
      if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
      else Iterator.empty
    }

    Pregel(spGraph, initialMessage)(vertexProgram, sendMessage, addMaps)
  }
}