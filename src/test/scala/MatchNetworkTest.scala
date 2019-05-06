import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{FlatSpec, Matchers}

class MatchNetworkTest extends FlatSpec with Matchers {

  classOf[MatchNetworkFunction].getSimpleName should "pass simple tests" in {
    "192.168.2.1" should    matchNetwork("192.168.2.1")
    "192.168.2.1" should    matchNetwork("192.168.2.1/32")
    "192.168.2.1" shouldNot matchNetwork("192.168.2.0/32")
    "192.168.2.1" should    matchNetwork("192.168.2.0/24")
    "2.16.162.72" should    matchNetwork("2.16.162.72/30")
    "2.16.162.73" should    matchNetwork("2.16.162.72/30")
    "2.16.162.76" shouldNot matchNetwork("2.16.162.72/30")
  }

  def matchNetwork(network: String) = NetworkMatcher(network: String)
}

case class NetworkMatcher(network: String) extends Matcher[String] {
  def apply(ip: String) = MatchResult(
    (new MatchNetworkFunction).matches(ip, network),
    s"$ip should match $network",
    s"$ip should not match $network"
  )
}


