import java.net.InetAddress

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{JavaBooleanObjectInspector, PrimitiveObjectInspectorFactory}
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{FlatSpec, Matchers}

class MatchNetworkTest extends FlatSpec with Matchers {

  implicit def desc = MatchNetworkFunction.desc
  def matchString(network: String) = StringToNetworkMatcher(network)
  def matchAddress(network: String) = AddressToNetworkMatcher(network)
  import NetworkMaskMatcher.ipToInt

  "NetworkMaskMatcher" should "parse IPv4 string" in {
    ipToInt("255.255.255.255") shouldBe 0xFFFFFFFF
    ipToInt("1.0.0.0")         shouldBe 0x01000000
    ipToInt("128.0.0.0")       shouldBe 0x80000000
    ipToInt("0.0.0.1")         shouldBe 0x00000001
  }

  it should "parse InetAddress instance" in {
    ipToInt(InetAddress.getByName("255.255.255.255")) shouldBe 0xFFFFFFFF
    ipToInt(InetAddress.getByName("1.0.0.0"))         shouldBe 0x01000000
    ipToInt(InetAddress.getByName("128.0.0.0"))       shouldBe 0x80000000
    ipToInt(InetAddress.getByName("0.0.0.1"))         shouldBe 0x00000001
    ipToInt(InetAddress.getByName("192.168.2.1"))     shouldBe ipToInt("192.168.2.1")
    ipToInt(InetAddress.getByName("2.16.162.72"))     shouldBe ipToInt("2.16.162.72")
  }

  it should "match string" in {
    "192.168.2.1" should    matchString("192.168.2.1")
    "192.168.2.1" should    matchString("192.168.2.1/32")
    "192.168.2.1" shouldNot matchString("192.168.2.0/32")
    "192.168.2.1" should    matchString("192.168.2.0/24")
    "2.16.162.72" should    matchString("2.16.162.72/30")
    "2.16.162.73" should    matchString("2.16.162.72/30")
    "2.16.162.76" shouldNot matchString("2.16.162.72/30")
  }


  it should "match InetAddress instance" in {
    InetAddress.getByName("192.168.2.1") should    matchAddress("192.168.2.1")
    InetAddress.getByName("192.168.2.1") should    matchAddress("192.168.2.1/32")
    InetAddress.getByName("192.168.2.1") shouldNot matchAddress("192.168.2.0/32")
    InetAddress.getByName("192.168.2.1") should    matchAddress("192.168.2.0/24")
    InetAddress.getByName("2.16.162.72") should    matchAddress("2.16.162.72/30")
    InetAddress.getByName("2.16.162.73") should    matchAddress("2.16.162.72/30")
    InetAddress.getByName("2.16.162.76") shouldNot matchAddress("2.16.162.72/30")
  }

  "MatchNetworkFunction" should "comply GenericUDF contract" in {
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

case class StringToNetworkMatcher(network: String)(implicit desc: FunctionDescriptor) extends Matcher[String] {
  def apply(ip: String) = MatchResult(
    NetworkMaskMatcher.matches(ip, network),
    s"$ip should match $network",
    s"$ip should not match $network"
  )
}

case class AddressToNetworkMatcher(network: String)(implicit desc: FunctionDescriptor) extends Matcher[InetAddress] {
  def apply(ip: InetAddress) = MatchResult(
    NetworkMaskMatcher.matches(ip, network),
    s"$ip should match $network",
    s"$ip should not match $network"
  )
}


