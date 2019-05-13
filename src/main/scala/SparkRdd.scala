import org.apache.spark.{SparkConf, SparkContext}

object SparkRdd extends App {

  val hdfsHost = "hdfs://localhost:9000" //"hdfs://quickstart.cloudera:8020/"

  def run(): Unit = {
    val conf = new SparkConf().setAppName("hdev")
    val sc = new SparkContext(conf)
    val input = sc.textFile(s"$hdfsHost/events/2019/05/13/FlumeData.1557749466924")
    val events = input.map(Event.parse)
    println(s"Loaded ${events.count()} events")
    sc.stop()
  }

  run()

}
