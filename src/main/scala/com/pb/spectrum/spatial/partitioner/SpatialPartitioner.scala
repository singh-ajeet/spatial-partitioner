package com.pb.spectrum.spatial.partitioner

import java.{lang => jl}

import org.apache.spark.Partitioner

final case class SpatialPartitioner(numOfGrids: Int) extends Partitioner {

  final override def numPartitions: Int = numOfGrids

  final override def getPartition(key: Any): Int = (key match {
    case x:Int => x
    case _ => throw new IllegalArgumentException("Invalid key, key should be Int type")
  })
}