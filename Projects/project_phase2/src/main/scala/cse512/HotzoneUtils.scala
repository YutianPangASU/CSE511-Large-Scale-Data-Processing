package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    try {
      val r = queryRectangle.split(',').map((x:String)=>x.toDouble)
      val p = pointString.split(',').map((x:String)=>x.toDouble)
      return p(0) >= r(0).min(r(2)) && p(0) <= r(0).max(r(2)) && p(1) >= r(1).min(r(3)) && p(1) <= r(1).max(r(3)) 
    } catch {
      case _:Throwable => return false
    }
  }

  // YOU NEED TO CHANGE THIS PART

}
