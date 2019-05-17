import FunctionDescriptor.ArgumentBuilder
import org.apache.hadoop.hive.ql.exec.{UDFArgumentException, UDFArgumentLengthException}
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector

import scala.reflect.ClassTag

trait NameDescriptor {
  def name: String
  override def toString = name
}

/** Meta info for a user-defined function */
case class FunctionDescriptor(functionName: String,
                              argumentNames: ArgumentBuilder[ObjectInspector]*)
  extends NameDescriptor {

  def name = functionName
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