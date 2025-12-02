import scala.io.{BufferedSource, Source}
import scala.util.Try

case class Booking( // U guys can add cols u need to use if needed
                    bookingId: String,
                    destinationCountry: String,
                    hotelName: String,
                    visitors: Int,
                    price: Double,
                    discount: Double,
                    profitMargin: Double
                  )

object HotelAnalysis:

  @main def run(): Unit =
    // define filename and load data
    val filename = "Hotel_Dataset.csv"
    val bookings = loadData(filename)
    // validation chcek
    if bookings.isEmpty then
      println(s"$bookings has no data")
    else
      // Expected 66541 data - 5 duplicates = 66536 unique bookings
//      println(bookings)
      println(s"loaded ${bookings.size} bookings.")
      println(bookings.take(5))

  def loadData(filename: String): List[Booking] =
    var source: BufferedSource = null

    try
      val resource = getClass.getClassLoader.getResource(filename)

      // validate resource
      if resource != null then {
//        println(s"resource.getPath: ${resource.getPath}")
        println("file found in resources")
        source = Source.fromURL(resource, "UTF-8")
      } else
        println("file NOT found in resources")

      // Read lines as list
      val lines = source.getLines().toList
      println(s"Debug: File successfully read. Total lines: ${lines.size}")

      // Simple parsing to verify data structure
      lines.drop(1).flatMap { line =>
        val cols = line.split(",").map(_.trim)
        // We just check if we have enough columns to proceed
        if cols.length >= 24 then
          Try {
            val bookingId = cols(0)
            val destCountry = cols(9)
            val hotel = cols(16)
            val visitors = cols(11).toInt
            val price = cols(20).toDouble
            // Handle the % sign
            val discountStr = cols(21).replace("%", "")
            val discount = discountStr.toDouble / 100.0
            val margin = cols(23).toDouble

            Booking(bookingId, destCountry, hotel, visitors, price, discount, margin)
          }.toOption
        else
          None
      }.distinct // removing only the 5 true duplicates

    catch
      case e: Exception =>
        println(s"errorrrrr: $e")
        List.empty
    finally {
      // closing file
      if source != null then
        println("success file source closed")
        source.close()
    }