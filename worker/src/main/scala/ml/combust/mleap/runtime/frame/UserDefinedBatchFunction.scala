package ml.combust.mleap.runtime.frame

import ml.combust.mleap.core.reflection.MleapReflection.{dataType, typeSpec}
import ml.combust.mleap.core.types.{DataType, StructType, TypeSpec}
import ml.combust.mleap.runtime.function.UserDefinedFunction

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

/** Companion object for creating user defined functions. */
object UserDefinedBatchFunction {
  def apply(f: AnyRef,
            bf: AnyRef,
            output: StructType,
            input: StructType): UserDefinedBatchFunction = {
    new UserDefinedBatchFunction(f, bf, output: TypeSpec, input.fields.map(_.dataType: TypeSpec))
  }

  def apply(f: AnyRef,
            bf: AnyRef,
            output: DataType,
            inputs: Seq[TypeSpec]): UserDefinedBatchFunction = {
    new UserDefinedBatchFunction(f, bf, output: TypeSpec, inputs)
  }

  def apply(f: AnyRef,
            bf: AnyRef,
            output: TypeSpec,
            input0: DataType,
            inputs: DataType *): UserDefinedBatchFunction = {
    new UserDefinedBatchFunction(f, bf, output, (input0 +: inputs).map(d => d: TypeSpec))
  }

  def apply(f: AnyRef,
            bf: AnyRef,
            output: DataType,
            input0: DataType,
            inputs: DataType *): UserDefinedBatchFunction = {
    new UserDefinedBatchFunction(f, bf, output, (input0 +: inputs).map(d => d: TypeSpec))
  }

  implicit def function0[RT: TypeTag](f: () => RT, bf: () => Seq[RT]): UserDefinedBatchFunction = {
    new UserDefinedBatchFunction(f, bf, typeSpec[RT], Seq())
  }

  implicit def function1[RT: TypeTag, T1: TypeTag](f: T1 => RT, bf: Seq[T1] => Seq[RT]): UserDefinedBatchFunction = {
    UserDefinedBatchFunction(f, bf, typeSpec[RT], dataType[T1])
  }

  implicit def function2[RT: TypeTag, T1: TypeTag, T2: TypeTag](f: (T1, T2) => RT, bf: (Seq[T1], Seq[T2]) => Seq[RT]): UserDefinedBatchFunction = {
    UserDefinedBatchFunction(f, bf, typeSpec[RT], dataType[T1], dataType[T2])
  }

  implicit def function3[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag](f: (T1, T2, T3) => RT, bf: (Seq[T1], Seq[T2], Seq[T3]) => Seq[RT]): UserDefinedBatchFunction = {
    UserDefinedBatchFunction(f, bf, typeSpec[RT], dataType[T1], dataType[T2], dataType[T3])
  }

  implicit def function4[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag](f: (T1, T2, T3, T4) => RT, bf: (Seq[T1], Seq[T2], Seq[T3], Seq[T4]) => Seq[RT]): UserDefinedBatchFunction = {
    UserDefinedBatchFunction(f, bf, typeSpec[RT], dataType[T1], dataType[T2], dataType[T3], dataType[T4])
  }

  implicit def function5[RT: TypeTag, T1: TypeTag, T2: TypeTag, T3: TypeTag, T4: TypeTag, T5: TypeTag](f: (T1, T2, T3, T4, T5) => RT, bf: (Seq[T1], Seq[T2], Seq[T3], Seq[T4], Seq[T5]) => Seq[RT]): UserDefinedBatchFunction = {
    UserDefinedBatchFunction(f, bf, typeSpec[RT], dataType[T1], dataType[T2], dataType[T3], dataType[T4], dataType[T5])
  }
}

class UserDefinedBatchFunction(f: AnyRef,
                               val bf: AnyRef,
                               output: TypeSpec,
                               inputs: Seq[TypeSpec]
                  ) extends UserDefinedFunction(f,output,inputs)
{
  override def copy(newF: AnyRef = f, newOutput: TypeSpec = output, newInputs: Seq[TypeSpec] = inputs): UserDefinedBatchFunction =
    new UserDefinedBatchFunction(newF, bf, newOutput, newInputs)

  override def withInputs(inputs: Seq[TypeSpec]): UserDefinedBatchFunction = copy(newInputs = inputs)
  override def withInputs(schema: StructType): UserDefinedBatchFunction = withDataTypeInputs(schema.fields.map(_.dataType))
  override def withDataTypeInputs(inputs: Seq[DataType]): UserDefinedBatchFunction = copy(newInputs = inputs.map(dt => dt: TypeSpec))

  override def withOutput(dt: DataType): UserDefinedBatchFunction = copy(newOutput = dt)
  override def withOutput(schema: StructType): UserDefinedBatchFunction = copy(newOutput = schema)
}
