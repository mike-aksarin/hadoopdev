import java.net.InetAddress
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import scala.util.Try

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

  def fromRow(row: Array[String]): Try[Event] = Try {
    Event(
      productName = row(0),
      productPrice = (BigDecimal(row(1)) * 100).toLong,
      purchaseDate = LocalDateTime.parse(row(2), dateTimeFormat),
      productCategory = row(3),
      clientIp = InetAddress.getByName(row(4))
    )
  }

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