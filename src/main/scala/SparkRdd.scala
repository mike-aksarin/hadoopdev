import java.io.FileInputStream
import java.net.InetAddress
import java.sql.{Connection, DriverManager}
import java.util.Properties
import java.util.logging.Logger

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag
import scala.util.{Success, Try}

object SparkRdd extends App {

  val settings = loadSettigns()
  val appName = setting("app.name", "HDEV")
  val topSize = setting("output.size", "10").toInt
  val hdfsHost = setting("hdfs.host", "hdfs://localhost:9000")
  val countryBlockFile = setting("csv.country.ip.file", "csv/GeoLite2-Country-Blocks-IPv4.csv")
  val countryNameFile = setting("csv.country.name.file", "csv/GeoLite2-Country-Locations-en.csv")
  val jdbcUrl = setting("jdbc.url", "jdbc:postgresql://localhost:5432/hdev")
  val categoriesSql = setting("sql.categories", "insert into top_categories(product_category, purchase_count) values(?, ?)")
  val productsSql = setting("sql.products", "insert into top_products(product_name, purchase_count) values(?, ?)")
  val countriesSql = setting("sql.countries", "insert into top_countries(country_name, spent_total) values(?, ?)")
  val cleanSql = setting("sql.clean", "truncate top_categories, top_products, top_countries")
  val log = Logger.getLogger(appName)

  def run(): Unit = {
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val events = parseEvents(sc).cache()
    val countriesByIp = parseCountriesByIp(sc)
    val count = events.count()
    val categories = topCategories(events).take(topSize)
    val products = topProducts(events).take(topSize)
    val countries = topCountries(events, countriesByIp).take(topSize)
    sc.stop()
    println(s"Loaded $count events")
    println(s"\nTop $topSize categories:\n${categories.mkString("\n")}")
    println(s"\nTop $topSize products:\n${products.mkString("\n")}")
    println(s"\nTop $topSize countries:\n${countries.mkString("\n")}")
    withJdbc { conn =>
      cleanTables(conn)
      val categoriesResult = exportTable(categories, categoriesSql, conn)
      println(s"\nInserted top $categoriesResult categories")
      val productsResult = exportTable(products, productsSql, conn)
      println(s"Inserted top $productsResult products")
      val countriesResult = exportTable(countries, countriesSql, conn)
      println(s"Inserted top $countriesResult countries")
    }

  }

  def parseEvents(sc: SparkContext): RDD[Event] = {
    val eventsInput = sc.textFile(s"$hdfsHost/events/2019/*/*/*")
    val events = parse(eventsInput, OpenCSV.parseEventLine, Event.fromRow)
    events
  }

  def parseCountriesByIp(sc: SparkContext): Broadcast[Map[String, String]] = {
    val countryBlockInput = sc.textFile(countryBlockFile)
    val countryBlockParser: Array[String] => Try[(String, Int)] = row => Try(row(0), toInt(row(1), -1))
    val countryBlocks = parse(countryBlockInput, OpenCSV.parseCountryLine, countryBlockParser)
    val countryBlocksFiltered = countryBlocks.filter { case (_, id) => id != -1 }
    val countryNamesInput = sc.textFile(countryNameFile)
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

  def topCountries(events: RDD[Event], countries: Broadcast[Map[String, String]]): RDD[(String, Long)] = {
    events
      .map(purchase => countryByIp(purchase.clientIp, countries) -> purchase.productPrice)
      .aggregateByKey(0L)(_ + _, _ + _)
      .sortBy(_._2, ascending = false)

  }

  def countryByIp(ip: InetAddress, countries: Broadcast[Map[String, String]]): String = {
    implicit val desc = FunctionDescriptor(appName)
    countries.value
      .collectFirst {
        case (mask, country) if NetworkMaskMatcher.matches(ip, mask) => country
      }
      .getOrElse("NO COUNTRY")
  }


  def loadSettigns(): Properties = {
    val props = new Properties()
    props.load(new FileInputStream(args.headOption.getOrElse("conf/application.conf")))
    props
  }

  def setting(key: String, default: String): String = settings.getProperty(key, default)

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
    val c = DriverManager.getConnection(jdbcUrl, settings)
    try block(c) finally c.close()
  }

  private def cleanTables(conn: Connection) = {
    val stmt = conn.createStatement()
    try stmt.execute(cleanSql) finally stmt.close()
  }

  private def exportTable(items: Seq[(String, Long)], sql: String, conn: Connection) = {
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

  run()

}
