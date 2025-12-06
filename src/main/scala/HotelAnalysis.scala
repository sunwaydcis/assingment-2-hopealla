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

//Calculation process
object MathUtils:
  // High-Performance: Find Min and Max in a SINGLE pass (O(N))
  // This is purely mathematical and has no dependency on "Bookings"
  def getRange[T](items: Iterable[T])(extractor: T => Double): (Double, Double) =
    if items.isEmpty then (0.0, 0.0)
    else
      items.foldLeft((Double.MaxValue, Double.MinValue)) { case ((min, max), item) =>
        val v = extractor(item)
        (math.min(min, v), math.max(max, v))
      }

  // Reusable Math for Normalization
  def normalize(value: Double, min: Double, max: Double, defaultVal: Double = 0.0): Double =
    if (max == min) defaultVal
    else (value - min) / (max - min)
end MathUtils

// Business logic here
object BookingExtensions:
  // Type alias for the common Hotel Key
  private type HotelKey = (String, String, String) // (Name, City, Country)

  // Specific business rule: How do we define a "Unique Hotel Group"?
  // Order: Name -> City -> Country
  def groupHotels(rows: List[Booking]): Map[HotelKey, List[Booking]] =
    rows.groupBy(b => (b.hotelName, b.destinationCity, b.destinationCountry))
end BookingExtensions

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

      //Remove any invisible BOM (Byte Order Mark) from the Column names.
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
        val cols = line.split(",", -1).map(_.trim)
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
    val hotelGroups = BookingExtensions.groupHotels(rows)

    val hotelStats = hotelGroups.map { case ((hotel, city, country), list) =>
      val avgPrice = list.map(b => b.price / (b.rooms * b.days)).sum / list.size
      val avgDisc = list.map(_.discount).sum / list.size
      val avgMarg = list.map(_.profitMargin).sum / list.size
      (hotel, city, country) -> HotelMetrics(avgPrice, avgDisc, avgMarg)
    }
    // Find Global Min/Max for Normalization
    val stats = hotelStats.values
    val (minP, maxP) = MathUtils.getRange(stats)(_.avgPricePerRoomDay)
    val (minD, maxD) = MathUtils.getRange(stats)(_.avgDiscount)
    val (minM, maxM) = MathUtils.getRange(stats)(_.avgMargin)

    // Calculate Normalized Score for each hotel
    // Formula: (Val - Min) / (Max - Min)
    // Low Price = Good (1 - norm)
    // High Disc = Good (norm)
    // Low Margin = Good (1 - norm)
    // Most economical = Highest Score
    val scoredHotels = hotelStats.map { case ((hotel, city, country), m) =>
      val normPrice = MathUtils.normalize(m.avgPricePerRoomDay, minP, maxP, 0.0)
      val normDisc = MathUtils.normalize(m.avgDiscount, minD, maxD, 1.0)
      val normMarg = MathUtils.normalize(m.avgMargin, minM, maxM, 0.0)

      val finalScore = (((1 - normPrice) + normDisc + (1 - normMarg)) * 100) / 3.0
      (s"$hotel ($city ,$country)", finalScore)
    }
    // using max to find the highest score
    scoredHotels.maxBy(_._2)
  end execute
end MostEconomicalHotel

// Question 3
object MostProfitableHotel extends BookingQuery[(String, Double)]:
  private case class PerformanceMetrics(totalVisitors: Int, avgMargin: Double)

  override def execute(rows: List[Booking]): (String, Double) =
    if rows.isEmpty then return ("No Data", 0.0)

    // Calculate avg margin total visitors
    val hotelGroups = BookingExtensions.groupHotels(rows)

    val hotelStats = hotelGroups.map { case ((hotel, city, country), list) =>
      val totalVisitors = list.map(_.visitors).sum
      val avgMargin = list.map(_.profitMargin).sum / list.size

      (hotel, city, country) -> PerformanceMetrics(totalVisitors, avgMargin)
    }

    // Find Min/Max for Normalization
    val stats = hotelStats.values
    val (minV, maxV) = MathUtils.getRange(stats)(_.totalVisitors.toDouble)
    val (minM, maxM) = MathUtils.getRange(stats)(_.avgMargin)

    // Calculate Normalized Score for each hotel
    // Formula: (Val - Min) / (Max - Min)
    // High Visitors = Good (norm)
    // High Margin = Good (norm)
    // Most profitable = Highest Score
    val scoredHotels = hotelStats.map { case ((hotel, city, country), m) =>
      val normVisitors = MathUtils.normalize(m.totalVisitors.toDouble, minV, maxV, 1.0)
      val normMargin = MathUtils.normalize(m.avgMargin, minM, maxM, 1.0)

      val finalScore = (normVisitors + normMargin) * 100 / 2.0
      (s"$hotel ($city, $country)", finalScore)
    }

    // 4. Sort Descending (Max Score is best)
    scoredHotels.maxBy(_._2)
  end execute
end MostProfitableHotel

class HotelReport(bookings: List[Booking]):
  //pass through any of the three )TopCountry, MostEconomicalHotel, MostProfitableHotel).
  def runQuery[T](query: BookingQuery[T]): T =
    query.execute(bookings)

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
      val (country, count) = report.runQuery(TopCountry)
      println(s"Top Country: $country ($count bookings)")

      // Q2
      println("\nQuestion 2:")
      val (econHotel, econScore) = report.runQuery(MostEconomicalHotel)
      println(s"Most Economical Hotel: $econHotel")
      println(f"Score: $econScore%.2f%%")


      // Q3
      println("\nQuestion 3:")
      val (profitHotel, profitScore) = report.runQuery(MostProfitableHotel)
      println(s"Best Performing Hotel: $profitHotel")
      println(f"Score: $profitScore%.2f%%")
  end run
end HotelAnalysis