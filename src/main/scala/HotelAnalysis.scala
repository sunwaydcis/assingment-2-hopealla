import scala.io.{BufferedSource, Source}
import scala.util.Try

case class Booking( // U guys can add cols u need to use if needed
                    bookingId: String,
                    destinationCountry: String,
                    destinationCity: String,
                    hotelName: String,
                    visitors: Int,
                    price: Double,
                    discount: Double,
                    profitMargin: Double,
                    days: Int,
                    rooms: Int
                  )

// Wrap in a module
object DataLoader:
  def loadData(filename: String): List[Booking] =
    var source: BufferedSource = null
    try
      val resource = getClass.getClassLoader.getResource(filename)
      // validate resource
      if resource != null then
      //println(s"resource.getPath: ${resource.getPath}")
        println("file found in resources")
        source = Source.fromURL(resource, "UTF-8")
      else
        println("file NOT found in resources")
        return List.empty

      // Read lines as list
      val lines = source.getLines().toList
      println(s"Debug: File successfully read. Total lines: ${lines.size}")
      if lines.isEmpty then return List.empty

      //Remove any invisible BOM (Byte Order Mark) from the Column names/
      val headerRow = lines.head.split(",").map(_.trim.replaceAll("\uFEFF", ""))
      //Dynamic approach to find data by name.
      //If columns shift or new columns are added, this map will update automatically.
      val headerMap = headerRow.zipWithIndex.toMap

      //Returns an explicit error if required column is missing from the file
      def getCol(name: String, row: Array[String]): String =
        headerMap.get(name) match
          case Some(index) if index < row.length => row(index)
          case Some(_) => throw new Exception(s"Row too short for col '$name'")
          case None => throw new Exception(s"Header '$name' NOT FOUND. Check spelling/case!")

      // Simple parsing to verify data structure.
      lines.tail.flatMap { line =>
        val cols = line.split(",").map(_.trim)
        //Using a more dynamic lookup for column names.
        if cols.nonEmpty then
          Try {
            val bookingId = getCol("Booking ID", cols)
            val destCountry = getCol("Destination Country", cols)
            val destCity = getCol("Destination City", cols)
            val hotel = getCol("Hotel Name", cols)
            val visitors = getCol("No. Of People", cols).toInt
            val price = getCol("Booking Price[SGD]", cols).toDouble
            // Handle the % sign
            val discountStr = getCol("Discount", cols).replace("%" , "")
            val discount = discountStr.toDouble / 100.00
            val margin = getCol("Profit Margin", cols).toDouble
            val days = getCol("No of Days", cols).toInt
            val rooms = getCol("Rooms", cols).toInt

            Booking(bookingId, destCountry, destCity, hotel, visitors, price, discount, margin, days, rooms)
          }.toOption
        else
          None
      }.distinct // removing only the 5 true duplicates
    catch
      case e: Exception =>
        println(s"error: $e")
        List.empty
    finally
      // closing file
      if source != null then
        println("success file source closed")
        source.close()
  end loadData
end DataLoader

trait BookingQuery[T]:
  def execute(rows: List[Booking]): T

// Question 1
object TopCountry extends BookingQuery[(String, Int)]:
  override def execute(rows: List[Booking]): (String, Int) =
    //Group by the destination country column
    //Returns the number of count for destination country
    val countryStats = rows.groupBy(_.destinationCountry).map {
      (country, list) => (country, list.size)
    }
    //Return highest number of booking by country if not empty.
    if countryStats.isEmpty then ("No data is found", 0) else countryStats.maxBy(_._2)
  end execute
end TopCountry

// Question 2
object MostEconomicalHotel extends BookingQuery[(String, Double)]:
  private case class HotelMetrics(avgPricePerRoomDay: Double, avgDiscount: Double, avgMargin: Double)

  override def execute(rows: List[Booking]): (String, Double) =
    if rows.isEmpty then return ("No Data", 0.0)
    // calculate avg of price, discount, profit of margin for each hotel
    // group by hotel name, country and city
    val hotelStats = rows.groupBy(b => (b.hotelName, b.destinationCountry, b.destinationCity)).map { case ((hotel, country, city), list) =>
      val avgPrice = list.map(b => b.price / (b.rooms * b.days)).sum / list.size
      val avgDisc = list.map(_.discount).sum / list.size
      val avgMarg = list.map(_.profitMargin).sum / list.size
      (hotel, country, city) -> HotelMetrics(avgPrice, avgDisc, avgMarg)
    }
    // Find Global Min/Max for Normalization
    val stats = hotelStats.values
    val minP = stats.map(_.avgPricePerRoomDay).min
    val maxP = stats.map(_.avgPricePerRoomDay).max
    val minD = stats.map(_.avgDiscount).min
    val maxD = stats.map(_.avgDiscount).max
    val minM = stats.map(_.avgMargin).min
    val maxM = stats.map(_.avgMargin).max

    // Calculate Normalized Score for each hotel
    // Formula: (Val - Min) / (Max - Min)
    // Low Price = Good (1 - norm)
    // High Disc = Good (norm)
    // Low Margin = Good (1 - norm)
    // Most economical = Highest Score
    val scoredHotels = hotelStats.map { case ((hotel, country, city), m) =>
      val normPrice = if (maxP == minP) 0.0 else (m.avgPricePerRoomDay - minP) / (maxP - minP)
      val normDisc = if (maxD == minD) 1.0 else (m.avgDiscount - minD) / (maxD - minD)
      val normMarg = if (maxM == minM) 0.0 else (m.avgMargin - minM) / (maxM - minM)

      val finalScore = (((1 - normPrice) + normDisc + (1 - normMarg)) * 100) / 3.0
      (s"$hotel ($city ,$country)", finalScore)
    }
    // using max to find the highest score
    scoredHotels.maxBy(_._2)

// Question 3
object MostProfitableHotel extends BookingQuery[(String, Double)]:
  private case class PerformanceMetrics(totalVisitors: Int, avgMargin: Double)

  override def execute(rows: List[Booking]): (String, Double) =
    if rows.isEmpty then return ("No Data", 0.0)

    // Calculate avg margin total visitors
    val hotelStats = rows.groupBy(b => (b.hotelName, b.destinationCity, b.destinationCountry)).map { case ((hotel, city, country), list) =>
      val totalVisitors = list.map(_.visitors).sum
      val avgMargin = list.map(_.profitMargin).sum / list.size

        (hotel, city, country) -> PerformanceMetrics(totalVisitors, avgMargin)
      }

    // Find Min/Max for Normalization
    val stats = hotelStats.values
    val minV = stats.map(_.totalVisitors).min.toDouble
    val maxV = stats.map(_.totalVisitors).max.toDouble
    val minM = stats.map(_.avgMargin).min
    val maxM = stats.map(_.avgMargin).max

    // Calculate Normalized Score for each hotel
    // Formula: (Val - Min) / (Max - Min)
    // High Visitors = Good (norm)
    // High Margin = Good (norm)
    // Most profitable = Highest Score
    val scoredHotels = hotelStats.map { case ((hotel, city, country), m) =>
      val normVisitors = if (maxV == minV) 1.0 else (m.totalVisitors - minV) / (maxV - minV)
      val normMargin = if (maxM == minM) 1.0 else (m.avgMargin - minM) / (maxM - minM)

      val finalScore = (normVisitors + normMargin) * 100 / 2.0
      (s"$hotel ($city, $country)", finalScore)
    }

    // 4. Sort Descending (Max Score is best)
    scoredHotels.maxBy(_._2)

class HotelReport(bookings: List[Booking]):
  def getTopCountryResult(): (String, Int) =
    TopCountry.execute(bookings)
  def getMostEconomicalHotel(): (String, Double) =
    MostEconomicalHotel.execute(bookings)
  def getMostProfitableHotel(): (String, Double) =
    MostProfitableHotel.execute(bookings)
end HotelReport

object HotelAnalysis:
  @main def run(): Unit =
    // define filename and load data
    val filename = "Hotel_Dataset.csv"
    val bookings = DataLoader.loadData(filename)
    // validation check
    if bookings.isEmpty then
      println(s"$bookings has no data")
    else
      // Expected 66541 data - 5 duplicates = 66536 unique bookings
      //      println(bookings)
      println(s"loaded ${bookings.size} bookings.")
      println(bookings.take(5))

      //answer display
      // Create report object
      val report = new HotelReport(bookings)

      // Q1
      println("\nQuestion 1:")
      val (country, count) = report.getTopCountryResult()
      println(s"Top Country: $country ($count bookings)")

      // Q2
      println("\nQuestion 2:")
      val (econHotel, econScore) = report.getMostEconomicalHotel()
      println(s"Most Economical Hotel: $econHotel")
      println(f"Score: $econScore%.2f%%")

      // Q3
      println("\nQuestion 3:")
      val (profitHotel, profitScore) = report.getMostProfitableHotel()
      println(s"Best Performing Hotel: $profitHotel")
      println(f"Score: $profitScore%.2f%%")
  end run
end HotelAnalysis
