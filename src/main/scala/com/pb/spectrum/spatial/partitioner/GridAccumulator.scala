package com.pb.spectrum.spatial.partitioner

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * To accumulate grids from all partitions
  *
  * @param accValue
  */
case class GridAccumulator(var accValue: mutable.HashMap[Int, Long] = new mutable.HashMap[Int, Long]()) extends AccumulatorV2[Int, mutable.HashMap[Int, Long]] {
  override def isZero: Boolean = accValue.isEmpty

  override def copy(): AccumulatorV2[Int, mutable.HashMap[Int, Long]] = new GridAccumulator(accValue)

  override def reset(): Unit = {
    accValue = new mutable.HashMap[Int, Long]()
  }

  override def add(gridId: Int): Unit = {
    val nextVal = if(accValue.contains(gridId)) accValue(gridId) + 1 else 1
    accValue += (gridId -> nextVal)
  }

  override def merge(other: AccumulatorV2[Int, mutable.HashMap[Int, Long]]): Unit = {
    for(x <- other.value){
      accValue.update(x._1, x._2)
    }
  }

  override def value: mutable.HashMap[Int, Long] = {
    accValue
  }
}
