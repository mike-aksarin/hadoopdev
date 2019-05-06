
import org.apache.hadoop.hive.ql.exec.{UDFArgumentException, UDFArgumentLengthException}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorFactory, StringObjectInspector}

import scala.reflect.ClassTag

class MatchNetworkFunction extends GenericUDF {

  val ipDesc = ArgumentDescriptor[StringObjectInspector]("ip")
  val networkDesc = ArgumentDescriptor[StringObjectInspector]("network")
  val desc = FunctionDescriptor("match_network", ipDesc, networkDesc)

  private var ipInspectorOpt: Option[StringObjectInspector] = None
  private var networkInspectorOpt: Option[StringObjectInspector] = None

  def ipInspector: StringObjectInspector = ipDesc.extractFromOption(ipInspectorOpt, desc)
  def networkInspector: StringObjectInspector = networkDesc.extractFromOption(networkInspectorOpt, desc)

  def getDisplayString(children: Array[String]): String = desc.functionDescStr

  def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    desc.validateArgumentCount(arguments)
    ipInspectorOpt = Some(ipDesc.validateArgument(arguments(0), desc))
    networkInspectorOpt = Some(networkDesc.validateArgument(arguments(1), desc))
    //function return type
    PrimitiveObjectInspectorFactory.javaBooleanObjectInspector
  }

  def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    val ip: String = ipInspector.getPrimitiveJavaObject(arguments(0))
    val network = networkInspector.getPrimitiveJavaObject(arguments(1))
    if (matches(ip, network)) java.lang.Boolean.TRUE else java.lang.Boolean.FALSE
  }

  def matches(ip: String, network: String): Boolean = {
    val (networkIp, maskLen) = parseNetworkMask(network)
    val mask = (0xFFFFFFFF00000000L >>> maskLen) & 0xFFFFFFFFL
    (ipToLong(ip) & mask) == (ipToLong(networkIp) & mask)
  }

  /** Parse strings like "2.16.162.72/30" to `(ip: String, maskLen: Int)` pair */
  private def parseNetworkMask(network: String) = {
    val parts = network.split("/")
    try {
      (parts(0), if (parts.length > 1) parts(1).toInt else 32)
    } catch {
      case e: NumberFormatException =>
        throw new HiveException(s"Could not parse network mask: " +
          s"$network for ${desc.functionName} function: ${e.getMessage}", e)
    }
  }

  private def ipToLong(ip: String): Long = {
    try {
      ip.split("\\.").foldLeft(0L) { (acc, part) => acc * 256 + part.toInt }
    } catch {
      case e: NumberFormatException =>
        throw new HiveException(s"Could not parse ip address: " +
        s"$ip for ${desc.functionName} function: ${e.getMessage}", e)
    }
  }

}

case class FunctionDescriptor(functionName: String, argumentDesc: ArgumentDescriptor[_]*) {

  def functionDescStr: String = s"$functionName($argNamesStr)"
  def argNamesStr: String = argumentDesc.map(_.argumentName).mkString
  def argLength: Int = argumentDesc.length

  def validateArgumentCount(arguments: Array[ObjectInspector]): Unit = {
    if (arguments.length != argumentDesc.length) {
      throw new UDFArgumentLengthException(s"`$functionName` takes $argLength arguments: $argNamesStr")
    }

  }
}

/** Argument descriptor for User-defined function in Hive. Currently accepts only simple arguments */
case class ArgumentDescriptor[T <: ObjectInspector : ClassTag](argumentName: String) {

  def validateArgument(argumentInspector: ObjectInspector, parent: FunctionDescriptor): T = {
    argumentInspector match {
      case expectedInspector: T => expectedInspector
      case _ => throw new UDFArgumentException(
        s"`$argumentName` argument has wrong type for a `${parent.functionName}` function." +           s" Should be suitable for ${scala.reflect.classTag[T]}")
    }
  }

  def extractFromOption(inspectorOpt: Option[T], parent: FunctionDescriptor): T = {
    throw new HiveException(s"function `${parent.functionName}`" +
      s"has not been initialized properly before evaluation:" +
      s"`$argumentName` argument inspector not found")
  }
}


