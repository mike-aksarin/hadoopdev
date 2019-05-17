import java.io.FileInputStream
import java.util.Properties

class Settings(args: Array[String]) {

  val properties = loadSettings()
  val appName = apply("app.name", "HDEV")
  val topSize = apply("output.size", "10").toInt
  val hdfsHost = apply("hdfs.host", "hdfs://localhost:9000")
  val eventsPath =  s"$hdfsHost/events/*/*/*/*"
  val countryBlockFile = apply("csv.country.ip.file", "csv/GeoLite2-Country-Blocks-IPv4.csv")
  val countryNameFile = apply("csv.country.name.file", "csv/GeoLite2-Country-Locations-en.csv")
  val jdbcUrl = apply("jdbc.url", "jdbc:postgresql://localhost:5432/hdev")

  def loadSettings(): Properties = {
    val props = new Properties()
    props.load(new FileInputStream(args.headOption.getOrElse("conf/application.conf")))
    props
  }

  def apply(key: String, default: String): String = properties.getProperty(key, default)
}

object Settings {

  def removeEscapedComma(line: String): String = {
    line.replaceAll("\\\\,", "")
  }
}
