import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{JavaBooleanObjectInspector, PrimitiveObjectInspectorFactory}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{FlatSpec, Matchers}

class MatchNetworkTest extends FlatSpec with Matchers {

  def matchNetwork(network: String) = NetworkMatcher(network)

  it should "pass simple tests" in {
    "192.168.2.1" should    matchNetwork("192.168.2.1")
    "192.168.2.1" should    matchNetwork("192.168.2.1/32")
    "192.168.2.1" shouldNot matchNetwork("192.168.2.0/32")
    "192.168.2.1" should    matchNetwork("192.168.2.0/24")
    "2.16.162.72" should    matchNetwork("2.16.162.72/30")
    "2.16.162.73" should    matchNetwork("2.16.162.72/30")
    "2.16.162.76" shouldNot matchNetwork("2.16.162.72/30")
  }

  it should "parse IPv4" in {
    val matcher = new MatchNetworkFunction
    matcher.ipToInt("255.255.255.255") shouldBe 0xFFFFFFFF
    matcher.ipToInt("1.0.0.0")         shouldBe 0x01000000
    matcher.ipToInt("128.0.0.0")       shouldBe 0x80000000
    matcher.ipToInt("0.0.0.1")         shouldBe 0x00000001
  }

  it should "comply GenericUDF contract" in {
    val matcher = new MatchNetworkFunction
    val resultInspector = matcher.initialize(Array(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector)).asInstanceOf[JavaBooleanObjectInspector]

    val result = matcher.evaluate(Array(
      new DeferredJavaObject("2.16.162.73"),
      new DeferredJavaObject("2.16.162.72/30")))

    resultInspector.get(result) shouldBe java.lang.Boolean.TRUE
  }

}

case class NetworkMatcher(network: String) extends Matcher[String] {
  def apply(ip: String) = MatchResult(
    (new MatchNetworkFunction).matches(ip, network),
    s"$ip should match $network",
    s"$ip should not match $network"
  )
}


