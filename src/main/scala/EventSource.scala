import java.io.StringWriter
import java.net.InetAddress
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.temporal.ChronoUnit
import java.util.{Timer, TimerTask}

import com.opencsv.{CSVWriter, ICSVWriter}

import scala.collection.JavaConverters._
import scala.util.Random

/** Event generator
  * product name — uniform
  * product price — gaussian
  * purchase date — time - gaussian, date - uniform, 1 week range
  * product category — uniform
  * client IP address — uniform, IPv4 random addresses
  */
object EventSource extends App {

  val period = 3000L

  runTimer(printNextBatch)

  def runTimer(task: => Unit):Unit = {
    val timer = new Timer()
    val timerTask = new TimerTask { override def  run = task }
    timer.schedule(timerTask, 0L, period)
    //timerTask.cancel()
  }

  def printNextBatch = {
    val batch = nextBatch()
    //log.info(batch)
    println(batch)
  }

  def nextBatch(): String = withWriter { csv =>
    val batch = Array.fill(nextBatchSize)(nextRow())
    csv.writeAll(batch.toIterable.asJava, false)
  }

  /** Generate next event as CSV string */
  def nextRow(): Array[String] = next().toRow

  /** Generate next event */
  def next(): Event = {
    val category = nextCategory()
    Event(
      nextProduct(category),
      nextPrice(),
      nextDate(),
      category,
      nextIp()
    )
  }

  def nextCategory(): String = uniform(Event.categories)

  def nextProduct(category: String): String = uniform(Event.products(category))

  def nextPrice(): Long = Math.abs((Random.nextGaussian() * Event.devPrice + Event.meanPrice).toLong)

  def nextDate(): LocalDateTime = {
    val date = LocalDate.now().minus(Random.nextInt(Event.dateRange) - 1, ChronoUnit.DAYS)
    val maxTime = 86399999999999L
    val meanTime = maxTime / 2
    val devTime = maxTime / 10
    val time = LocalTime.ofNanoOfDay(Math.abs(Random.nextGaussian() * devTime + meanTime).toLong % maxTime)
    date.atTime(time)
  }

  def nextIp(): InetAddress = {
    //InetAddress.getLocalHost
    val bytes = new Array[Byte](4)
    Random.nextBytes(bytes)
    InetAddress.getByAddress(bytes)
  }

  def nextBatchSize: Int = Random.nextInt(15000)

  def uniform[T](values: Array[T]): T = {
    values(Random.nextInt(values.length))
  }

  def withWriter(block: CSVWriter => Unit): String = {
    val buf = new StringWriter()
    val writer = new CSVWriter(buf,
      ICSVWriter.DEFAULT_SEPARATOR,
      ICSVWriter.NO_QUOTE_CHARACTER,
      '\\',
      ICSVWriter.DEFAULT_LINE_END)
    block(writer)
    writer.close()
    buf.getBuffer.toString
  }

}
