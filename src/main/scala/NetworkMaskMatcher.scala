import java.net.InetAddress

object NetworkMaskMatcher {

  /** check whether IP v4 address matches subnetwork address */
  def matches(ip: String, network: String)(implicit desc: FunctionDescriptor): Boolean = {
    val (networkIp, mask) = parseNetworkMask(network)
    (ipToInt(ip) & mask) == (ipToInt(networkIp) & mask)
  }

  /** check whether IP address matches subnetwork address */
  def matches(ip: InetAddress, network: String)(implicit desc: FunctionDescriptor): Boolean = {
    val (networkIp, mask) = parseNetworkMask(network)
    val ipBits = ipToInt(ip) & mask
    val networkBits = ipToInt(networkIp) & mask
    ipBits == networkBits
  }


  /** Parse strings like "2.16.162.72/30" to `(ip: String, mask: Int)` pair */
  private def parseNetworkMask(network: String)(implicit desc: FunctionDescriptor) = {
    val parts = network.split("/")
    try {
      val ip = parts(0)
      val maskLen = if (parts.length > 1) parts(1).toInt else 32
      val mask = ((0xFFFFFFFF00000000L >>> maskLen) & 0xFFFFFFFFL).toInt
      (ip, mask)
    } catch {
      case e: NumberFormatException =>
        throw new IllegalArgumentException(s"${desc.functionName} function could not parse network mask: " +
          s"$network: ${e.getMessage}", e)
    }
  }

  /** Parse IP v4 and pack its 4 bytes into Int */
  def ipToInt(ip: String)(implicit desc: FunctionDescriptor): Int = {
    try {
      ip.split("\\.").foldLeft(0) { (acc, part) => acc << 8 | part.toInt }
    } catch {
      case e: NumberFormatException =>
        throw new IllegalArgumentException(s"${desc.functionName} function could not parse ip address: " +
          s"$ip: ${e.getMessage}", e)
    }
  }

  def ipToInt(ip: InetAddress): Int = {
    ip.getAddress.foldLeft(0) { (acc, part) => acc << 8 | (part & 0xFF) }
  }

}
