import java.net.InetAddress
import java.sql.{Connection, DriverManager}
import java.util.logging.Logger

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag
import scala.util.{Success, Try}

object SparkRdd extends App {

  val settings = new Settings(args)
  val log = Logger.getLogger(SparkRdd.getClass.getSimpleName)

  def run(): Unit = withSpark { sc =>
    val events = parseEvents(sc).cache()
    val countriesByIp = parseCountriesByIp(sc)
    val count = events.count()
    val categories = topCategories(events).take(settings.topSize)
    val products = topProducts(events).take(settings.topSize)
    val countries = topCountries(events, countriesByIp).take(settings.topSize)
    println(s"Loaded $count events")
    println(s"\nTop ${categories.size} categories:\n${categories.mkString("\n")}")
    println(s"\nTop ${products.size} products:\n${products.mkString("\n")}")
    println(s"\nTop ${countries.size} countries:\n${countries.mkString("\n")}")
    withJdbc { conn =>
      cleanTables(conn)
      val categoriesResult = exportCountTable(categories, categoriesSql, conn)
      println(s"\nInserted top $categoriesResult categories")
      val productsResult = exportCountTable(products, productsSql, conn)
      println(s"Inserted top $productsResult products")
      val countriesResult = exportSumTable(countries, countriesSql, conn)
      println(s"Inserted top $countriesResult countries")
    }
  }


  def withSpark(body: SparkContext => Unit): Unit = {
    val conf = new SparkConf().setAppName(settings.appName)
    val sc = new SparkContext(conf)
    try body(sc) finally sc.stop()
  }

  def parseEvents(sc: SparkContext): RDD[Event] = {
    val eventsInput = sc.textFile(settings.eventsPath)
    parse(eventsInput, OpenCSV.parseEventLine, Event.fromRow)
  }

  def parseCountriesByIp(sc: SparkContext): Broadcast[Map[String, String]] = {
    val countryBlockInput = sc.textFile(settings.countryBlockFile)
    val countryBlockParser: Array[String] => Try[(String, Int)] = row => Try(row(0), toInt(row(1), -1))
    val countryBlocks = parse(countryBlockInput, OpenCSV.parseCountryLine, countryBlockParser)
    val countryBlocksFiltered = countryBlocks.filter { case (_, id) => id != -1 }
    val countryNamesInput = sc.textFile(settings.countryNameFile)
    val countryNameParser: Array[String] => Try[(Int, String)] = row => Try(row(0).toInt, row(5))
    val countryNames = parse(countryNamesInput, OpenCSV.parseCountryLine, countryNameParser)
    val countryNameById = countryNames.collectAsMap()
    val countryByIp = countryBlocksFiltered.collectAsMap().mapValues(countryNameById)
    sc.broadcast(countryByIp.toMap)
  }

  def topCategories(events: RDD[Event]): Seq[(String, Long)] = {
    val categoryMap = events.map(_.productCategory).countByValue()
    categoryMap.toSeq.sortBy(_._2)(reverseOrd)
  }

  def topProducts(events: RDD[Event]): Seq[(String, Long)] = {
    val productMap = events.map(_.productName).countByValue()
    productMap.toSeq.sortBy(_._2)(reverseOrd)
  }

  def topCountries(events: RDD[Event], countries: Broadcast[Map[String, String]]): RDD[(String, BigDecimal)] = {
    events
      .map(purchase => countryByIp(purchase.clientIp, countries) -> purchase.price)
      .filter(_._1.nonEmpty)
      .aggregateByKey(BigDecimal(0))(_ + _, _ + _)
      .sortBy(_._2, ascending = false)
  }

  def countryByIp(ip: InetAddress, countries: Broadcast[Map[String, String]]): String = {
    implicit val name = NetworkMaskMatcher.defaultName
    countries.value
      .collectFirst {
        case (mask, country) if NetworkMaskMatcher.matches(ip, mask) => country
      }
      .getOrElse("")
  }


  def parse[T: ClassTag](lines: RDD[String], csvParser: String => Array[String], itemParser: Array[String] => Try[T]): RDD[T] = {
    lines.map { line =>
      val tryItem = Try(csvParser(line)).flatMap(itemParser)
      tryItem.failed.foreach(e => log.warning(s"Wrong input '$line' $e"))
      tryItem
    } collect {
      case Success(item) => item
    }
  }

  def simpleCsvParser: String => Array[String] =  _.split(",\\s*")

  def toInt(str: String, default: Int): Int = {
    if (str == "") default else str.toInt
  }

  def reverseOrd[T](implicit ordering: Ordering[T]): Ordering[T] = ordering.reverse

  def withJdbc(block: Connection => Unit): Unit = {
    val c = DriverManager.getConnection(settings.jdbcUrl, settings.properties)
    try block(c) finally c.close()
  }

  val categoriesSql = settings("sql.categories", "insert into top_categories(product_category, purchase_count) values(?, ?)")
  val productsSql = settings("sql.products", "insert into top_products(product_name, purchase_count) values(?, ?)")
  val countriesSql = settings("sql.countries", "insert into top_countries(country_name, spent_total) values(?, ?)")
  val cleanSql = settings("sql.clean", "truncate top_categories, top_products, top_countries")

  private def cleanTables(conn: Connection) = {
    val stmt = conn.createStatement()
    try stmt.execute(cleanSql) finally stmt.close()
  }

  private def exportCountTable(items: Seq[(String, Long)], sql: String, conn: Connection) = {
    val stmt = conn.prepareStatement(sql)
    try {
      items.foreach { case (key, value) =>
        stmt.setString(1, key)
        stmt.setLong(2, value)
        stmt.addBatch()
      }
      stmt.executeBatch().sum
    } finally {
      stmt.close()
    }
  }

  private def exportSumTable(items: Seq[(String, BigDecimal)], sql: String, conn: Connection) = {
    val stmt = conn.prepareStatement(sql)
    try {
      items.foreach { case (key, value) =>
        stmt.setString(1, key)
        stmt.setBigDecimal(2, value.bigDecimal)
        stmt.addBatch()
      }
      stmt.executeBatch().sum
    } finally {
      stmt.close()
    }
  }

  run()

}
