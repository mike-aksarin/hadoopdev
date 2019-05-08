
import FunctionDescriptor.ArgumentBuilder
import org.apache.hadoop.hive.ql.exec.{UDFArgumentException, UDFArgumentLengthException}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorFactory, StringObjectInspector}

import scala.reflect.ClassTag

/** Hive user-defined function to check whether IP v4 address matches subnetwork address */
class MatchNetworkFunction extends GenericUDF {

  private def desc = MatchNetworkFunction.desc

  private var argumentInspectors = desc.emptyInspectors

  def getDisplayString(children: Array[String]): String = desc.functionDescStr

  def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    argumentInspectors = desc.validateArguments(arguments)
    PrimitiveObjectInspectorFactory.javaBooleanObjectInspector //function return type
  }

  def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    val ip = desc.argumentDesc(0).extractString(arguments(0), argumentInspectors(0))
    val network = desc.argumentDesc(1).extractString(arguments(1), argumentInspectors(1))
    if (matches(ip, network)) java.lang.Boolean.TRUE else java.lang.Boolean.FALSE
  }

  /** check whether IP v4 address matches subnetwork address */
  def matches(ip: String, network: String): Boolean = {
    val (networkIp, maskLen) = parseNetworkMask(network)
    val mask = ((0xFFFFFFFF00000000L >>> maskLen) & 0xFFFFFFFFL).toInt
    (ipToInt(ip) & mask) == (ipToInt(networkIp) & mask)
  }

  /** Parse strings like "2.16.162.72/30" to `(ip: String, maskLen: Int)` pair */
  private def parseNetworkMask(network: String) = {
    val parts = network.split("/")
    try {
      (parts(0), if (parts.length > 1) parts(1).toInt else 32)
    } catch {
      case e: NumberFormatException =>
        throw new HiveException(s"${desc.functionName} function could not parse network mask: " +
          s"$network: ${e.getMessage}", e)
    }
  }

  /** Parse IP v4 and pack its 4 bytes into Int */
  def ipToInt(ip: String): Int = {
    try {
      ip.split("\\.").foldLeft(0) { (acc, part) => acc << 8 | part.toInt }
    } catch {
      case e: NumberFormatException =>
        throw new HiveException(s"User-defined function  ${desc.functionName} could not parse ip address: " +
        s"$ip: ${e.getMessage}", e)
    }
  }

}

object MatchNetworkFunction {
  val desc = FunctionDescriptor("match_network",
    FunctionDescriptor.argument[StringObjectInspector]("ip"),
    FunctionDescriptor.argument[StringObjectInspector]("network"))
}

/** Meta info for a user-defined function */
case class FunctionDescriptor(functionName: String, argumentNames: ArgumentBuilder[ObjectInspector]*) {

  val argumentDesc = argumentNames.map(name => ArgumentDescriptor(name))

  def functionDescStr: String = s"$functionName($argNamesStr)"
  def argNamesStr: String = argumentNames.map(_.argumentName).mkString
  def argLength: Int = argumentNames.length

  def emptyInspectors: Array[Option[ObjectInspector]] = Array.fill(argLength)(None)

  def validateArguments(argInspectors: Array[ObjectInspector]): Array[Option[ObjectInspector]] = {
    validateArgumentCount(argInspectors)
    argInspectors.zip(argumentDesc).map { case (insp, desc) =>
      Option(desc.validateArgument(insp))
    }
  }

  def validateArgumentCount(argInspectors: Array[ObjectInspector]): Unit = {
    if (argInspectors.length != argLength) {
      throw new UDFArgumentLengthException(s"`$functionName` takes $argLength arguments: $argNamesStr")
    }
  }

  /** Meta info for a user-defined function's argument. Currently supports simple arguments */
  case class ArgumentDescriptor[+T <: ObjectInspector : ClassTag](name: ArgumentBuilder[T]) {

    def validateArgument(argumentInspector: ObjectInspector): T = {
      argumentInspector match {
        case expectedInspector: T => expectedInspector
        case _ => throw new UDFArgumentException(
          s"`${name.argumentName}` argument has wrong type for a User-defined function `$functionName`." +
          s" Should be suitable for ${scala.reflect.classTag[T]}")
      }
    }

    def extractString(arg: GenericUDF.DeferredObject, inspectorOpt: Option[_]): String = inspectorOpt match {
      case None => throw new EmptyInspectorError
      case Some(stringInspector: StringObjectInspector) => stringInspector.getPrimitiveJavaObject(arg.get)
      case _ => throw new WrongArgumentType[String]
    }

    class EmptyInspectorError extends HiveException(
      s"User-defined function `$functionName` " +
      s"has not been initialized properly before evaluation: " +
      s"`${name.argumentName}` argument inspector not found")

    class WrongArgumentType[E : ClassTag] extends HiveException(
      s"User-defined function `$functionName` " +
      s"could not cast argument of ${scala.reflect.classTag[T]} to ${scala.reflect.classTag[E]}"
    )
  }
}

object FunctionDescriptor {
  def argument[T <: ObjectInspector : ClassTag](name: String) = ArgumentBuilder[T](name)

  case class ArgumentBuilder[+T <: ObjectInspector : ClassTag](argumentName: String)
}
