package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    // pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART
    pickupInfo = pickupInfo.select("*").where(f"x >= $minX%f AND x <= $maxX%f AND y >= $minY%f AND y <= $maxY%f AND z >= $minZ%d AND z <= $maxZ%d")

    var cellDataDf = pickupInfo.groupBy("x", "y", "z").agg(count("*").as("cnt"))
    cellDataDf.createOrReplaceTempView("celldata")

    // Compute average x and std
    val avgX = (cellDataDf.select("cnt").agg(sum("cnt")).first().getLong(0).toDouble) / numCells
    val stdX = scala.math.sqrt((cellDataDf.withColumn("squarecnt", pow("cnt", 2)).select("squarecnt").agg(sum("squarecnt")).first().getDouble(0) / numCells) - scala.math.pow(avgX, 2))

    // Compute sum(neighbor counts) with Spark SQL
    var neighborCnt = spark.sql("SELECT cell.x AS x, cell.y AS y, cell.z AS z, sum(neighbor.cnt) AS neighborcount "
          + "FROM celldata AS cell, celldata AS neighbor "
          + "WHERE (neighbor.x = cell.x OR neighbor.x = cell.x - 1 OR neighbor.x = cell.x + 1) AND (neighbor.y = cell.y OR neighbor.y = cell.y - 1 OR neighbor.y = cell.y + 1) AND (neighbor.z = cell.z OR neighbor.z = cell.z - 1 OR neighbor.z = cell.z + 1) "
          + "GROUP BY cell.x, cell.y, cell.z")


    // compute g score (do two functions because udf accepts at most 10 args)
    var funcCountNeighbor = udf((x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int)
    => HotcellUtils.computeNumNeighbors(x, y, z, minX, maxX, minY, maxY, minZ, maxZ))

    var countNeighborResult = neighborCnt.withColumn("wsum", funcCountNeighbor(col("x"),
      col("y"), col("z"), lit(minX.toInt), lit(maxX.toInt), lit(minY.toInt), lit(maxY.toInt),
      lit(minZ.toInt), lit(maxZ.toInt)))

    var funcGScore = udf((x: Int, y: Int, z: Int, wsum: Int, neighborCnt: Int, nCells: Int, avgX: Double, stdX: Double)
    => HotcellUtils.computeGScore(x, y, z, wsum, neighborCnt, nCells, avgX, stdX))
    
    var gScoreResult = countNeighborResult.withColumn("gscore", funcGScore(col("x"), col("y"),
      col("z"), col("wsum"), col("neighborcount"), lit(numCells.toInt), lit(avgX.toDouble), lit(stdX.toDouble))).
      orderBy(desc("gscore")).limit(50).select("x", "y", "z")

    return gScoreResult // YOU NEED TO CHANGE THIS PART
  }
}
