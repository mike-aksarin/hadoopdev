import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Try

object SparkRdd extends App {

  val appName = "HDEV"
  val topSize = 10
  val log = java.util.logging.Logger.getLogger(appName)
  val hdfsHost = "hdfs://localhost:9000" //"hdfs://quickstart.cloudera:8020/"

  def run(): Unit = {
    val conf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(conf)
    val input = sc.textFile(s"$hdfsHost/events/2019/05/13/FlumeData.1557749466924")
    val events = input.map(Event.parse).cache()
    val countMessage = events.count()
    val categoriesMessage = topCategories(events).take(topSize).mkString("\n")
    val productsMessage = topProducts(events).take(topSize).mkString("\n")
    sc.stop()
    println(s"Loaded $countMessage events")
    println(s"Top $topSize categories:\n$categoriesMessage")
    println(s"Top $topSize products:\n$productsMessage")
  }

  private def topCategories(events: RDD[Try[Event]]) = {
    val categoryMap = events.map(field(_.productCategory)).countByValue()
    categoryMap.toSeq.sortBy(_._2)(reverse[Long])
  }

  private def topProducts(events: RDD[Try[Event]]) = {
    val productMap = events.map(field(_.productName)).countByValue()
    productMap.toSeq.sortBy(_._2)(reverse[Long])
  }

  def field(key: Event => String)(tryEvent: Try[Event]) = {
    tryEvent.failed.foreach(e => log.warning(s"Wrong input: $e"))
    val field = tryEvent.map(key).getOrElse("WRONG INPUT")
    field
  }

  def reverse[T](implicit ordering: Ordering[T]): Ordering[T] = ordering.reverse



  run()

}
