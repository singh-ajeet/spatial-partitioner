package com.pb.spectrum.spatial.partitioner

import java.util.List
import java.{util, lang => jl}

import com.vividsolutions.jts.geom.{Envelope, Geometry}
import com.vividsolutions.jts.index.strtree.STRtree
import com.vividsolutions.jts.io.WKTReader

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object HelperUtilities {
  def appendElementToArray(input:Array[String], newElement: Any) : Array[Any] = {
    input :+ newElement
  }

  /**
    * This function returns envelope as an array of Double
    * Array[MinX, MaxX, MinY, MaxY]
    *
    * @param wkt
    * @return
    */
  def findEnvelope(wkt: String): Array[Double] = {
    val jtsEnvelope = new WKTReader().read(wkt).getEnvelopeInternal
    Array(jtsEnvelope.getMinX, jtsEnvelope.getMaxX, jtsEnvelope.getMinY, jtsEnvelope.getMaxY)
  }

  def seqOp(buffer : Array[Double], input: Array[Any]): Array[Double] = {
    val bufferEnvelope = new Envelope(buffer(0), buffer(1), buffer(2), buffer(3))
    val envelope:Array[Double] = input.last.asInstanceOf[Array[Double]]
    val inputEnvelope = new Envelope(envelope(0), envelope(1), envelope(2), envelope(3))

    bufferEnvelope.expandToInclude(inputEnvelope)
    Array(bufferEnvelope.getMinX, bufferEnvelope.getMaxX, bufferEnvelope.getMinY, bufferEnvelope.getMaxY)
  }

  def combineOp(leftBuffer : Array[Double], rightBuffer : Array[Double]): Array[Double] = {
    val leftBufferEnvelope = new Envelope(leftBuffer(0), leftBuffer(1), leftBuffer(2), leftBuffer(3))
    val rightBufferEnvelope = new Envelope(rightBuffer(0), rightBuffer(1), rightBuffer(2), rightBuffer(3))

    leftBufferEnvelope.expandToInclude(rightBufferEnvelope)
    Array(leftBufferEnvelope.getMinX, leftBufferEnvelope.getMaxX, leftBufferEnvelope.getMinY, leftBufferEnvelope.getMaxY)
  }

  /**
    * One spatial record can intersects multiple grids, so it retruns list
    *  (ID1, record)
    *  (ID2, record)
    *  (ID3, record)
    *
    * @param gridIndex
    * @param record
    * @return
    */
  def assignGridId(gridIndex: STRtree, record: Array[Any]) : List[(Int, Array[Any])]= {
    val bounds = record.last.asInstanceOf[Array[Double]]
    val found:util.List[Int] = gridIndex.query(new Envelope(bounds(0), bounds(1), bounds(2), bounds(3))).asInstanceOf[util.List[Int]]

    var result:ListBuffer[(Int, Array[Any])]  = ListBuffer()
    for(id <- found){
      result += ((id.toInt, record))
    }
    result.toList
  }

  def arrayToString(elements: Array[Any]): String ={
    val stringBuffer = new StringBuilder()

    for(i:Int <- 0 to elements.size-1) {
      stringBuffer.append(elements(i))
      if(i < elements.size-1) {
        stringBuffer.append('\t')
      }
    }
    stringBuffer.toString
  }

  def pointInPolygon(polygonWkt: String, pointWkt: String): Boolean = {
    val jtsPolygon = new WKTReader().read(polygonWkt)
    val jtsPoint = new WKTReader().read(pointWkt)

    if(jtsPolygon.contains(jtsPoint)) {
      return true
    } else {
      false
    }
  }

  def pointInPolygon(boundaries: Map[Geometry, String], poi:Geometry): String = {
    var riskScore = ""
    boundaries.foreach(boundary => {
      if(boundary._1.contains(poi)) {
        riskScore = boundary._2
      }
    })
    riskScore
  }

  def pointInPolygon(index: STRtree, poi: Geometry): String = {
    val queryResult:util.List[(Geometry, String)] = index.query(poi.getEnvelopeInternal).asInstanceOf[util.List[(Geometry, String)]]
    var riskScore = ""
    for(tuple <- queryResult) {
      if(tuple._1.contains(poi)){
        riskScore = tuple._2
      }
    }
    riskScore
  }

  def arrayToGeometryMap(boundaries: Iterable[Array[Any]], boundaryWktColIndex: Int,  riskColIndex: Int): Map[Geometry, String] = {
    val wktReader: WKTReader = new WKTReader()
    var polygons = mutable.Map.empty[Geometry, String]
    boundaries.foreach(boundary =>
      polygons += (wktReader.read(boundary(boundaryWktColIndex).asInstanceOf[String]) -> boundary(riskColIndex).asInstanceOf[String])
    )
    polygons.toMap
  }

  def arrayToRTree(boundaries: Iterable[Array[Any]], boundaryWktColIndex: Int,riskColIndex: Int): STRtree = {
    val wktReader: WKTReader = new WKTReader()
    val index: STRtree = new STRtree
    boundaries.foreach(boundary => {
      val bounds = boundary.last.asInstanceOf[Array[Double]]
      val polygon = wktReader.read(boundary(boundaryWktColIndex).asInstanceOf[String])
      val risk = boundary(riskColIndex).asInstanceOf[String]
      index.insert(new Envelope(bounds(0), bounds(1), bounds(2), bounds(3)), new Tuple2(polygon, risk))
    })
    index
  }

  def pointInPolygonInBatch(boundaries: Iterable[Array[Any]], insuredEntities: Iterable[Array[Any]],
                            poiWktColIndex: Int,
                            boundaryWktColIndex: Int,
                            riskColIndex: Int,
                            geometryThresold: Int): List[String] = {
    val result: ListBuffer[String] = new ListBuffer()
    val wktReader: WKTReader = new WKTReader()

    if(boundaries.size > geometryThresold){
      val index: STRtree = arrayToRTree(boundaries, boundaryWktColIndex, riskColIndex)
      insuredEntities.foreach(insuredEntity => {
        /**
          * We dont required Bounding box anymore So replacing it with Risk score
          */
        val point = wktReader.read(insuredEntity(poiWktColIndex).asInstanceOf[String])
        insuredEntity(insuredEntity.size -1) = pointInPolygon(index, point)
        result += arrayToString(insuredEntity)
      })

    } else {
      val polygons = arrayToGeometryMap(boundaries, boundaryWktColIndex, riskColIndex)
      insuredEntities.foreach(insuredEntity => {
        /**
          * We dont required Bounding box anymore So replacing it with Risk score
          */
        val point = wktReader.read(insuredEntity(poiWktColIndex).asInstanceOf[String])
        insuredEntity(insuredEntity.size -1) = pointInPolygon(polygons.toMap, point)
        result += arrayToString(insuredEntity)
      })
    }
    result.toList
  }

  def arrayToString(boundaries: Iterable[Array[Any]], poi: Array[Any]) = {
    val stringBuffer = new StringBuffer()
    val poiList = poi.toList
    for(i <- 0 to(poi.size - 2)){
      stringBuffer.append(poiList.get(i) + "\t")
    }
    stringBuffer.toString
  }
}
