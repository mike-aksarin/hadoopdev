import java.io.FileInputStream
import java.net.InetAddress
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
  val log = Logger.getLogger(appName)

  def run(): Unit = {
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val events = parseEvents(sc).cache()
    val countries = parseCountriesByIp(sc)
    val countMessage = events.count()
    val categoriesMessage = topCategories(events).take(topSize).mkString("\n")
    val productsMessage = topProducts(events).take(topSize).mkString("\n")
    val countriesMessage = topCountries(events, countries).take(topSize).mkString("\n")
    sc.stop()
    println(s"Loaded $countMessage events")
    println(s"Top $topSize categories:\n$categoriesMessage")
    println(s"Top $topSize products:\n$productsMessage")
    println(s"Top $topSize countries:\n$countriesMessage")
  }

  def parseEvents(sc: SparkContext): RDD[Event] = {
    val eventsInput = sc.textFile(s"$hdfsHost/events/2019/05/13/FlumeData.1557749466924")
    val events = parse(eventsInput, OpenCSV.eventFun, Event.fromRow)
    events
  }

  def parseCountriesByIp(sc: SparkContext): Broadcast[Map[String, String]] = {
    val countryBlockInput = sc.textFile(countryBlockFile)
    val countryBlockParser: Array[String] => Try[(String, Int)] = row => Try(row(0), toInt(row(1), -1))
    val countryBlocks = parse(countryBlockInput, OpenCSV.countryFun, countryBlockParser)
    val countryBlocksFiltered = countryBlocks.filter { case (_, id) => id != -1 }
    val countryNamesInput = sc.textFile(countryNameFile)
    val countryNameParser: Array[String] => Try[(Int, String)] = row => Try(row(0).toInt, row(5))
    val countryNames = parse(countryNamesInput, OpenCSV.countryFun, countryNameParser)
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
      .getOrElse("COUNTRY NOT FOUND")
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

  run()

}
