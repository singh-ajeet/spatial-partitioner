package com.pb.spectrum.spatial.partitioner

import scala.collection.mutable.ListBuffer

case class Grid(envelopeArray: Array[Double], id:Int) {
  require(envelopeArray.nonEmpty, "envelope cant be empty")
  require(id >= 0, "grid id cant be less than 0")

  /**
    * This is just for debug purpose only
    *
    * @return
    */
  @transient
  lazy val wkt =
    s"POLYGON((${envelopeArray(0)} ${envelopeArray(3)},${envelopeArray(1)} ${envelopeArray(3)},${envelopeArray(1)} ${envelopeArray(2)},${envelopeArray(0)} ${envelopeArray(2)},${envelopeArray(0)} ${envelopeArray(3)}))"

}