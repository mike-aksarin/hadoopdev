import java.io.StringWriter
import java.net.InetAddress
import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.logging.Logger
import java.util.{Timer, TimerTask}

import com.opencsv.CSVWriter

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
  //val log = Logger.getLogger("")

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
    csv.writeAll(batch.toIterable.asJava)
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
    val writer = new CSVWriter(buf)
    block(writer)
    writer.close()
    buf.getBuffer.toString
  }

}

case class Event(
                  productName: String,
                  productPrice: Long,
                  purchaseDate: LocalDateTime,
                  productCategory: String,
                  clientIp: InetAddress
                ) {
  def toRow = Array(
    productName,
    (BigDecimal(productPrice) / 100).toString(),
    purchaseDate.format(Event.dateTimeFormat),
    productCategory,
    clientIp.getHostAddress)
}

object Event {

  val dateTimeFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  val dateRange = 7
  val meanPrice = 10000
  val devPrice = 3000

  val products = Map(
    "Martin Odersky" -> Array(
      "Scala by Example",
      "Functional Programming Principles in Scala",
      "Functional Program Design in Scala",
      "Simplicitly: foundations and applications of implicit function types",
      "A Nominal Theory of Objects with Dependent Types",
      "The Essence of Dependent Object Types",
      "Initialization patterns in Dotty"
    ),
    "Martin Fowler" -> Array(
      "UML Distilled: A Brief Guide to the Standard Object Modeling Language",
      "Refactoring: Improving the Design of Existing Code",
      "Domain-Specific Languages"
    ),
    "Robert C. Martin" -> Array(
      "Clean Code: A Handbook of Agile Software Craftsmanship",
      "Clean Architecture: A Craftsman's Guide to Software Structure and Design"
    ),
    "Joshua Bloch" -> Array(
      "Effective Java: Programming Language Guide",
      "Java Concurrency in Practice",
      "Java Puzzlers: Traps, Pitfalls, and Corner Cases"
    ),
    "Arkady and Boris Strugatsky" -> Array(
      "Noon: 22nd Century",
      "Far Rainbow",
      "Hard to Be a God",
      "Monday Begins on Saturday",
      "The Ugly Swans",
      "The Inhabited Island",
      "Dead Mountaineer's Hotel",
      "Space Mowgli",
      "Roadside Picnic",
      "Definitely Maybe",
      "The Doomed City",
      "Beetle in the Anthill",
      "The Time Wanderers",
      "Overburdened with Evil",
      "Space Apprentice"
    ),
    "Victor Pelevin" -> Array(
      "Hermit and Sixfinger",
      "The Yellow Arrow",
      "The Life of Insects",
      "Buddha's Little Finger",
      "Generation P",
      "The Sacred Book of the Werewolf",
      "The Helmet of Horror",
      "Empire V",
      "t",
      "S.N.U.F.F.",
      "Batman Apollo",
      "Love for three Zuckerbrins",
      "The Watcher",
      "Methusela's lamp, or The last battle of chekists with masons",
      "Secret Views of Mount Fuji"
    ),
    "Richard Phillips Feynman" -> Array(
      "Surely You're Joking, Mr. Feynman!",
      "What Do You Care What Other People Think?",
      "The Pleasure of Finding Things Out",
      "Six Easy Pieces: Essentials of Physics Explained by Its Most Brilliant Teacher",
      "Six Not So Easy Pieces: Einstein's Relativity, Symmetry and Space-Time",
      "The Character of Physical Law"
    ),
    "John Ronald Reuel Tolkien" -> Array(
      "The Hobbit",
      "The Lord of the Rings",
      "The Silmarillion"
    ),
    "Fyodor Dostoevsky" -> Array(
      "The Idiot",
      "Demons",
      "The Brothers Karamazov"
    ),
    "Leo Tolstoy" -> Array(
      "War and Peace",
      "Resurrection",
      "Hadji Murat",
      "Sevastopol Sketches",
      "What I Believe",
      "The Kingdom of God Is Within You",
      "The Law of Love and the Law of Violence",
      "I can't be silent"
    ),
    "Anton Chekhov" -> Array(
      "My Life. The story of a provincial",
      "Gooseberries",
      "A Medical Case",
      "The Darling",
      "Doctor Startsev",
      "Peasants",
      "In the Ravine"
    )
  )

  val categories = products.keys.toArray
}
