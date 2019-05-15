
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.{PrimitiveObjectInspectorFactory, StringObjectInspector}

/** Hive user-defined function to check whether IP v4 address matches subnetwork address */
class MatchNetworkFunction extends GenericUDF {

  private implicit def desc = MatchNetworkFunction.desc

  private var argumentInspectors = desc.emptyInspectors

  def getDisplayString(children: Array[String]): String = desc.functionDescStr

  def initialize(arguments: Array[ObjectInspector]): ObjectInspector = {
    argumentInspectors = desc.validateArguments(arguments)
    PrimitiveObjectInspectorFactory.javaBooleanObjectInspector //function return type
  }

  def evaluate(arguments: Array[GenericUDF.DeferredObject]): AnyRef = {
    val ip = desc.argumentDesc(0).extractString(arguments(0), argumentInspectors(0))
    val network = desc.argumentDesc(1).extractString(arguments(1), argumentInspectors(1))
    if (NetworkMaskMatcher.matches(ip, network)) {
      java.lang.Boolean.TRUE
    } else {
      java.lang.Boolean.FALSE
    }
  }

}

object MatchNetworkFunction {
  val desc = FunctionDescriptor("match_network",
    FunctionDescriptor.argument[StringObjectInspector]("ip"),
    FunctionDescriptor.argument[StringObjectInspector]("network"))
}

