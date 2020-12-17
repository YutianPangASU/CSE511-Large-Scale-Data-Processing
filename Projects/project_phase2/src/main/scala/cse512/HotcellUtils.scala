package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def computeNumNeighbors(x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int): Int = {
    // first determine the number of neighbors (include itself)
    var nNeighbor = 0
    if(x == minX || x == maxX) {
      nNeighbor += 1
    }
    if(y == minY || y == maxY) {
      nNeighbor += 1
    }
    if(z == minZ || z == maxZ) {
      nNeighbor += 1
    }

    if(nNeighbor == 0) {
      // not on boundary
      nNeighbor = 26 + 1
    } else if(nNeighbor == 1) {
      nNeighbor = 17 + 1
    } else if(nNeighbor == 2) {
      nNeighbor = 11 + 1
    } else {
      // nNeighbor == 3
      nNeighbor = 7 + 1
    } 
    return nNeighbor.toInt
  }

  // YOU NEED TO CHANGE THIS PART
  def computeGScore(x: Int, y: Int, z: Int, wsum: Int, neighborCnt: Int, nCells: Int, avgX: Double, stdX: Double): Double = {
        return (neighborCnt.toDouble - avgX * wsum.toDouble) / (stdX * scala.math.sqrt((nCells.toDouble * wsum.toDouble - (wsum.toDouble * wsum.toDouble)) / (nCells.toDouble - 1.0)))
  }
}
