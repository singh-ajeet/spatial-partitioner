package com.pb.spectrum.spatial.partitioner

import java.{util, lang => jl}

import com.opencsv.CSVParserBuilder
import com.pb.spectrum.spatial.partitioner.HelperUtilities._
import com.vividsolutions.jts.geom.Envelope
import com.vividsolutions.jts.index.strtree.STRtree
import org.apache.commons.cli.Option
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

object SpatialPartitionerSparkApp extends App {
  @transient lazy val log = Logger.getLogger("SpatialPartitionerLogger")

  private val OPTION_INPUT = new Option("input", true, "The HDFS path to the input directory")
  private val OPTION_INPUT_REFRENCE_DATA = new Option("referenceData", true, "The HDFS path to the reference data directory")
  private val OPTION_OUTPUT = new Option("output", true, "The HDFS path to the output directory")
  private val OPTION_NUM_OF_PARTITIONS = new Option("partitions", true, "Number of partitions to split reference data")
  private val ALL_OPTIONS = util.Arrays.asList(OPTION_INPUT, OPTION_INPUT_REFRENCE_DATA, OPTION_OUTPUT, OPTION_NUM_OF_PARTITIONS)

  private val GRID_MAX_NUM_OF_GEOMETRIES: Int = 20
  private val SPLIT_LARGER_GRID_TO_PARTITIONS = 5
  private val GEOMETRY_ITERATION_THRESHOLD = 10;

/*
  System.setProperty("spark.master", "local[*]")
  System.setProperty("spark.app.name", "Spark Spatial Partitioner App")
*/

  //val DELIMITER = '\t'
  //  val INPUT_RISK_REFERENCE_DATA = "data\\fire_risk_boundries_TAB.txt"
  //val RISK_DESC_COL_INDEX: Integer = 0
  //  val INPUT_POI_DATA = "data\\BOB.txt"
  // val OUTPUT_LOCATION = "data\\RiskScores_output"
 // val INPUT_WKT_COL_INDEX: Integer = 11
 // val BOUNDARY_WKT_COL_INDEX: Int = 2
  //  val NUM_OF_PARTITIONS = 50

  val parser = new ApplicationOptionsParser("spark-submit --class " + getClass.getName + " --master yarn --deploy-mode cluster [JAR_PATH]", new Configuration, args, ALL_OPTIONS, true)
  val configuration = parser.getConfiguration

  val input = configuration.get(OPTION_INPUT.getOpt)
  val referenceData = configuration.get(OPTION_INPUT_REFRENCE_DATA.getOpt)
  val output = configuration.get(OPTION_OUTPUT.getOpt)
  val partitions = configuration.get(OPTION_NUM_OF_PARTITIONS.getOpt)

  val session = SparkSession.builder.getOrCreate
  log.info("Spark session started.")

  try{
    /**
      * Cleanup - Delete if output exists
      */
    val outputPath = new Path(output)
    val fs = FileSystem.get(outputPath.toUri, new Configuration())
    if(fs.exists(outputPath)){
      fs.delete(outputPath, true)
    }
    /**
      * Step 1: Read POI data and persist RDD to avoid re processing
      */
    val poiDataset = session
      .sparkContext
      .textFile(input)
      .map(x => {
        val csvParser = new CSVParserBuilder()
          .withSeparator('\t')
          .withQuoteChar('"')
          .withEscapeChar('\u0000')
          .build()
        csvParser.parseLine(x)
      })
      .map(x => appendElementToArray(x, findEnvelope(x(11))))
      .persist

    /**
      * Step 2: Find aggregated envelope
      */
    val aggregatedEnvelope = poiDataset.aggregate[Array[Double]](poiDataset.first.last.asInstanceOf[Array[Double]])(
      (buffer, input) => seqOp(buffer, input),
      (leftBuffer, rightBuffer) => combineOp(leftBuffer, rightBuffer)
    )

    log.info("Aggregated Envelope: " + new Grid(aggregatedEnvelope, 1).wkt)

    /**
      * Step 3: Split aggregated envelope to grids
      */
    val grids = GridUtilities.getGrids(aggregatedEnvelope, Integer.valueOf(partitions))
    val index: STRtree = new STRtree
    grids.foreach(g => {
      index.insert(new Envelope(g.envelopeArray(0), g.envelopeArray(1), g.envelopeArray(2), g.envelopeArray(3)), g)
    })

    log.info("Grid Index size before filtering it :" + index.size())
    log.info("Generated grids before filtering: " + GridUtilities.toWKT(grids))

    /**
      * Step 4: Broadcast R-Tree index to remove unnecessary grids
      */
    val gridAccumulator = GridAccumulator()
    session.sparkContext.register(gridAccumulator)

    poiDataset.foreachPartition(geometries => {
      geometries.foreach(geom => {
        val bounds = geom.last.asInstanceOf[Array[Double]]
        val grids:util.List[Grid] = index.query(new Envelope(bounds(0), bounds(1), bounds(2), bounds(3))).asInstanceOf[util.List[Grid]]
        for(grid:Grid <- grids){
          gridAccumulator.add(grid.id)
        }
      })
    })

    val accumulatedGrids = gridAccumulator.value
    val filteredGrids = grids.filter(g => accumulatedGrids.contains(g.id))

    log.info("WKT of generated grids After filtering: " + GridUtilities.toWKT(filteredGrids.toList))

    /**
      * Step 5: Assign a sequence number to each grids, this number will be used as partition ID/number
      */
    var gridNumber: Int = 0
    val filteredGridIndex: STRtree = new STRtree

    filteredGrids.foreach(g => {
      filteredGridIndex.insert(new Envelope(g.envelopeArray(0), g.envelopeArray(1), g.envelopeArray(2), g.envelopeArray(3)), gridNumber)
      gridNumber = gridNumber + 1
    })

    log.info("Grid Index size After filtering it :" + filteredGridIndex.size())

    val boundariesDataset = session
      .sparkContext
      .textFile(referenceData)
      .map(x => {
        val csvParser = new CSVParserBuilder()
          .withSeparator('\t')
          .withQuoteChar('"')
          .withEscapeChar('\u0000')
          .build()
        csvParser.parseLine(x)
      })
      .map(x => appendElementToArray(x, findEnvelope(x(2))))
      .persist // We need persist it to avoid re processing in multiple parallel steps

    /**
      * Step 6: Now partition data by using grids
      */
    val spatialPartitioner = new SpatialPartitioner(gridNumber)

    val partitionedBoundaryDataset = boundariesDataset
      .flatMap(geom => assignGridId(filteredGridIndex, geom))
      .partitionBy(spatialPartitioner)
      .groupByKey()

    val partitionedPOIDataset = poiDataset
      .flatMap(geom => assignGridId(filteredGridIndex, geom))
      .partitionBy(spatialPartitioner)
      .groupByKey()

    /**
      * Step 7: Perform join

      * For co-located and co-grouped join  -
      *  both Datasets should have same Partitioner and should went through same set of transformations
      *
      * Return an RDD containing all pairs of elements with matching keys in this and other. Each
      * pair of elements will be returned as a (k, (v1, v2)) tuple, where (k, v1) is in this and
      * (k, v2) is in other. Uses the given Partitioner to partition the output RDD.
      */
    log.info("Boundary data partitioner " + partitionedBoundaryDataset.partitioner)
    log.info("POI data partitioner " + poiDataset.partitioner)

    partitionedBoundaryDataset.join(partitionedPOIDataset)
      .flatMap(x => pointInPolygonInBatch(x._2._1, x._2._2, 11, 2, 0, GEOMETRY_ITERATION_THRESHOLD ))
      .saveAsTextFile(output)

    /**
      * Perform Find Nearest
      */
    //partitionedBoundaryDataset.join(partitionedPOIDataset)

  }finally {
    //  session.close
    log.info("Spark session closed.")
  }
}