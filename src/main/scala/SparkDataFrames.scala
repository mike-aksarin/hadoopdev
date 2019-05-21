import java.util.logging.Logger

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

object SparkDataFrames extends App {

  val settings = new Settings(args)
  val log = Logger.getLogger(SparkDataFrames.getClass.getSimpleName)

  def run(): Unit = withSpark { implicit spark =>
    val events = loadEvents().cache()
    val count = events.count()
    val categories = countByField(events, "product_category").cache()
    val products = countByField(events, "product_name").cache()
    val countries = topCountries(events).cache()

    writeJdbc(categories, "top_categories")
    writeJdbc(products, "top_products")
    writeJdbc(countries, "top_countries")

    println(s"Loaded $count events")
    categories.show()
    products.show()
    countries.show()
  }

  def withSpark(body: SparkSession => Unit): Unit = {
    val spark = SparkSession
      .builder()
      .appName(settings.appName)
      .getOrCreate()
    try body(spark) finally spark.close()
  }

  private def loadEvents()(implicit spark: SparkSession): Dataset[EventRow] = {
    spark.read
      .option("mode", "DROPMALFORMED")
      .schema(EventRow.schema)
      .csv(loadEventLines())
      .as[EventRow]
  }

  private def loadEventLines()(implicit spark: SparkSession) = {
    import spark.implicits._
    spark.sparkContext
      .textFile(settings.eventsPath)
      .map(Settings.removeEscapedComma)
      .toDS()
  }

  private def countByField(events: Dataset[EventRow], field: String): DataFrame = {
    events
      .groupBy(field)
      .count()
      .withColumnRenamed("count", "purchase_count")
      .orderBy(desc("purchase_count"))
      .limit(settings.topSize)
  }

  private def topCountries(events: Dataset[EventRow])(implicit spark: SparkSession): DataFrame = {
    val countryBlocks = loadPlain(settings.countryBlockFile)
    val countryNames = loadPlain(settings.countryNameFile)
    val countryByIpMask = countryByIp(countryBlocks, countryNames)
    val Array(ipMask, countryName) = countryByIpMask.columns
    events
      .join(countryByIpMask)
      .where(networkMatch(events("client_ip"), countryByIpMask(ipMask)))
      .select("product_price", countryName)
      .groupBy(countryName)
      .sum("product_price")
      .withColumnRenamed("sum(product_price)", "spent_total")
      .orderBy(desc("spent_total"))
      .limit(settings.topSize)
  }

  private def countryByIp(countryBlocks: DataFrame, countryNames: DataFrame)
                         (implicit spark: SparkSession): Dataset[(String, String)] = {
    import spark.implicits._
    countryBlocks
      .join(countryNames)
      .where(countryBlocks(countryBlocks.columns(1)) === countryNames(countryNames.columns(0)))
      .select(countryBlocks.columns(0), countryNames.columns(5))
      .as[(String, String)]
  }

  val networkMatch: UserDefinedFunction = {
    implicit val name = NetworkMaskMatcher.defaultName
    udf((ip: String, mask: String) => NetworkMaskMatcher.matches(ip, mask))
  }

  def loadPlain(fileName: String)(implicit spark: SparkSession) = {
    spark.read
      .option("header", "true")
      .csv(fileName)
  }

  def writeJdbc(data: DataFrame, table: String) = {
    data.write
      .mode(SaveMode.Overwrite)
      .jdbc(settings.jdbcUrl, table, settings.properties)
  }

  run()
}