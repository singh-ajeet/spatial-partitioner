package com.pb.spectrum.spatial.partitioner

import scala.collection.mutable.ListBuffer

object GridUtilities {
  /**
    * Utility method to create grids with provided envelope and number of partitions
    *
    * @param envelopeArray
    * @param partitions
    * @return List of Grid objects
    */
  def getGrids(envelopeArray: Array[Double],partitions:Int ): List[Grid] = {
    require(envelopeArray.nonEmpty, "envelope cant be empty")
    require(partitions > 0, "number of partitions should be greater than zero")

    val partitionsAxis = math.sqrt(partitions + 1).intValue
    val intervalX:Double = (envelopeArray(1) - envelopeArray(0))/partitionsAxis
    val intervalY:Double = (envelopeArray(3) - envelopeArray(2))/partitionsAxis

    val grids:ListBuffer[Grid] = new ListBuffer[Grid]
    var gridId = 0
    for(i:Int <- 0 to partitionsAxis -1){
      for (j:Int <- 0 to partitionsAxis -1){
        val grid = new Grid(Array(envelopeArray(0) + intervalX * i,
          envelopeArray(0) + intervalX * (i + 1),
          envelopeArray(2) + intervalY * j,
          envelopeArray(2) + intervalY * (j + 1)), gridId)

        grids += grid
        gridId = gridId+1
      }
    }
    grids.toList
  }

 /* /**
    * create grids by using geotools API
    *
    * @param envelopeArray
    * @param partitions
    * @return
    */
  def getGridsWithGeotools(envelopeArray: Array[Double],partitions:Int ): List[Grid] = {
    require(envelopeArray.nonEmpty, "envelope cant be empty")
    require(partitions > 0, "partitions must be greater than 0")

    val gridBounds = new ReferencedEnvelope(envelopeArray(0), envelopeArray(1), envelopeArray(2), envelopeArray(3),
                      DefaultGeographicCRS.WGS84)
    println("Bounds after converting to geotools: " + new Grid(Array(gridBounds.getMinX, gridBounds.getMaxX, gridBounds.getMinY, gridBounds.getMaxY), 10000).wkt)
    val gridsFeatureSource = Grids.createSquareGrid(gridBounds, gridSideLength(envelopeArray, partitions))
    val gridsIterator = gridsFeatureSource.getFeatures.features
    val grids:ListBuffer[Grid] = new ListBuffer[Grid]
    var gridId = 0
    while(gridsIterator.hasNext){
      val bounds = gridsIterator.next.getBounds
      grids += new Grid(Array(bounds.getMinX, bounds.getMaxX, bounds.getMinY, bounds.getMaxY), gridId)
      gridId = gridId+1
    }
    grids.toList
  }
*/
  def gridSideLength(envelopeArray : Array[Double], partitions: Int): Double = {
    require(envelopeArray.nonEmpty, "envelope cant be empty")
    require(partitions > 0, "partitions must be greater than 0")

    val rootOfPartitions = Math.sqrt(partitions).toInt
    val dx = envelopeArray(1) - envelopeArray(0)
    val dy = envelopeArray(3) - envelopeArray(2)
    math.max(dx, dy)/rootOfPartitions
  }

  /**
    * This is for debug purpose only, to verify the generated grids
    *
    * @param grids
    * @return
    */
  def toWKT(grids: List[Grid]): String = {
    require(grids.nonEmpty, "grids cant be empty")

    val stringBuilder = new StringBuilder("MULTIPOLYGON (")
    var i:Int = 0
    grids.foreach(g => {
      stringBuilder.append(g.wkt.replace("POLYGON", "") )
      i += 1
      if(i != grids.size){
        stringBuilder.append(", ")
      }
    })

    stringBuilder.append(")")
    stringBuilder.toString
  }
}
