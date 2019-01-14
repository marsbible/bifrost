package org.bifrost.tools

import resource._
import org.fusesource.scalate._
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.MleapContext.defaultContext
import ml.combust.bundle.BundleFile
import ml.combust.mleap.core.types.{TensorShape, _}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import ml.combust.mleap.tensor.Tensor
import resource.managed

import scala.collection.mutable.{ArrayBuffer, ListBuffer, WrappedArray}

/**
  *
  * val schema = StructType(StructField("features", TensorType(DoubleType())),
  * StructField("name", StringType()),
  * StructField("list_data", ListType(StringType())),
  * StructField("nullable_double", DoubleType(true)),
  * StructField("float", FloatType(false)),
  * StructField("byte_tensor", TensorType(ByteType(false))),
  * StructField("short_list", ListType(ShortType(false))),
  * StructField("nullable_string", StringType(true))).get
  * val dataset = LocalDataset(Row(Tensor.denseVector(Array(20.0, 10.0, 5.0)),
  * "hello", Seq("hello", "there"),
  * Option(56.7d), 32.4f,
  *   Tensor.denseVector(Array[Byte](1, 2, 3, 4)),
  * Seq[Short](99, 12, 45),
  * None))
  * val frame = LeapFrame(schema, dataset
  * * */

case class ModelInfo(file: String) {
  //超过此数会报错Too many arguments in method signature
  val MaxFieldNumer = 120

  def camelify(name : String): String = {
    def loop(x : List[Char]): List[Char] = (x: @unchecked) match {
      case '_' :: '_' :: rest => loop('_' :: rest)
      case '_' :: c :: rest => Character.toUpperCase(c) :: loop(rest)
      case '_' :: Nil => Nil
      case c :: rest => c :: loop(rest)
      case Nil => Nil
    }
    if (name == null)
      ""
    else
      loop('_' :: name.toList).mkString
  }

  def camelifyMethod(name: String): String = {
    val tmp: String = camelify(name)
    if (tmp.length == 0)
      ""
    else
      tmp.substring(0,1).toLowerCase + tmp.substring(1)
  }

  private var mleapPipeline: (Transformer,Array[String]) = (for(bf <- managed(BundleFile("jar:file:"+ file))) yield {
    (bf.loadMleapBundle().get.root, bf.readNote("output_fields").split(','))
  }).tried.get

  val engine = new TemplateEngine

  def transform(frame: DefaultLeapFrame): DefaultLeapFrame = {
    mleapPipeline._1.transform(frame).get
  }

  def inputSchema(): Seq[StructField] = {
    mleapPipeline._1.inputSchema.fields
  }

  def outputSchema(): Seq[StructField] = {
    mleapPipeline._1.outputSchema.select(mleapPipeline._2: _*).get.fields
  }

  private def encodeMleapTypeToPb(inputType: BasicType): String = {
    /**
      * pb不支持byte,short类型，统一映射到int32，接收端需要根据schema转换
      对应的scala类型如下：
      * scala.Boolean
      * scala.Int
      * scala.Long
      * scala.Float
      * scala.Double
      * scala.Predef.String
      * com.google.protobuf.ByteString
    */
    inputType match {
      case BasicType.Boolean => "bool"
      case BasicType.Byte => "sint32"
      case BasicType.Short => "sint32"
      case BasicType.Int => "sint32"
      case BasicType.Long => "sint64"
      case BasicType.Float => "float"
      case BasicType.Double => "double"
      case BasicType.String => "string"
      case BasicType.ByteString => "bytes"
      case _ => {
        assert(false)
        ""
      }
    }
  }

  private def decodePbValueToRow(inputType: BasicType, name: String): String = {
    val requestName = "request." + name
    inputType match {
      case BasicType.Boolean => requestName
      case BasicType.Byte => requestName+".toByte"
      case BasicType.Short => requestName+".toShort"
      case BasicType.Int =>  requestName
      case BasicType.Long => requestName
      case BasicType.Float => requestName
      case BasicType.Double => requestName
      case BasicType.String => requestName
      case BasicType.ByteString => "ml.combust.mleap.tensor.ByteString("+requestName+".toByteArray)"
      case _ => {
        assert(false)
        ""
      }
    }
  }

  private def decodePbListValueToRow(inputType: BasicType, name: String): String = {
    val requestName = "request." + name
    inputType match {
      case BasicType.Boolean => requestName
      case BasicType.Byte => requestName+".map(i => i.toByte)"
      case BasicType.Short => requestName+".map(i => i.toShort)"
      case BasicType.Int =>  requestName
      case BasicType.Long => requestName
      case BasicType.Float => requestName
      case BasicType.Double => requestName
      case BasicType.String => requestName
      case BasicType.ByteString => requestName+".map(i => ml.combust.mleap.tensor.ByteString(i.toByteArray))"
      case _ => {
        assert(false)
        ""
      }
    }
  }

  private def formatTensorStr(name: String, map: String, dimensions: Seq[Int]) = {
    val idx = camelifyMethod(name+"_indices")

    f"if(!request.$idx%s.isEmpty) Tensor.create(request.$name%s.$map%stoArray, $dimensions, Option(request.$idx%s.map(i => i.index))) " + f"else Tensor.create(request.$name%s.$map%stoArray, $dimensions)"
  }

  private def decodePbTensorValueToRow(inputType: BasicType, name: String, dimensions: Seq[Int]): String = {
    val requestName = formatTensorStr(name, "", dimensions)

    inputType match {
      case BasicType.Boolean => requestName
      case BasicType.Byte => formatTensorStr(name, "map(i => i.toByte).", dimensions)
      case BasicType.Short => formatTensorStr(name, "map(i => i.toShort).", dimensions)
      case BasicType.Int =>  requestName
      case BasicType.Long => requestName
      case BasicType.Float => requestName
      case BasicType.Double => requestName
      case BasicType.String => requestName
      case BasicType.ByteString => formatTensorStr(name, "map(i => ml.combust.mleap.tensor.ByteString(i.toByteArray)).", dimensions)
      case _ => {
        assert(false)
        ""
      }
    }
  }

  private def encodeRowValueToPb(inputType: BasicType, id: Int): String = {
    val idstr = "(" + id+ ")"

    inputType match {
      case BasicType.Boolean => "row.getBool"+idstr
      case BasicType.Byte =>"row.getInt"+idstr
      case BasicType.Short => "row.getInt"+idstr
      case BasicType.Int =>  "row.getInt"+idstr
      case BasicType.Long => "row.getLong"+idstr
      case BasicType.Float => "row.getFloat"+idstr
      case BasicType.Double => "row.getDouble"+idstr
      case BasicType.String => "row.getString"+idstr
      case BasicType.ByteString => "com.google.protobuf.ByteString.copyFrom("+"row.getByteString"+idstr+".bytes"+")"
      case _ => {
        assert(false)
        ""
      }
    }
  }


  private def formatRowListStr(id: Int, typestr: String): String = {
    f"row.getSeq[$typestr%s]($id%d)"
  }

  private def encodeRowListValueToPb(inputType: BasicType, id: Int): String = {
    inputType match {
      case BasicType.Boolean => formatRowListStr(id, "Boolean")
      case BasicType.Byte => formatRowListStr(id, "Byte") + ".map(i => i.toInt)"
      case BasicType.Short => formatRowListStr(id, "Short") + ".map(i => i.toInt)"
      case BasicType.Int =>  formatRowListStr(id, "Int")
      case BasicType.Long => formatRowListStr(id, "Long")
      case BasicType.Float => formatRowListStr(id, "Float")
      case BasicType.Double => formatRowListStr(id, "Double")
      case BasicType.String => formatRowListStr(id, "String")
      case BasicType.ByteString => formatRowListStr(id, "ml.combust.mleap.tensor.ByteString") + ".map(i => com.google.protobuf.ByteString.copyFrom(i.bytes))"
      case _ => {
        assert(false)
        ""
      }
    }
  }

  private def formatRowTensorStr(id: Int, typestr: String): String = {
    f"row.getTensor[$typestr%s]($id%d).rawValues.toSeq"
  }

  private def formatRowTensorIndicesStr(id: Int, typestr: String): String = {
    f"if(row.getTensor[$typestr%s]($id%d).isSparse) row.getTensor[$typestr%s]($id%d).asInstanceOf[SparseTensor[$typestr%s]].indices.map(i => DataIndex(i)) else Seq.empty"
  }

  private def encodeRowTensorValueToPb(inputType: BasicType, id: Int): String = {
    inputType match {
      case BasicType.Boolean => formatRowTensorStr(id, "Boolean")
      case BasicType.Byte => formatRowTensorStr(id, "Byte") + ".map(i => i.toInt)"
      case BasicType.Short => formatRowTensorStr(id, "Short") + ".map(i => i.toInt)"
      case BasicType.Int =>  formatRowTensorStr(id, "Int")
      case BasicType.Long => formatRowTensorStr(id, "Long")
      case BasicType.Float => formatRowTensorStr(id, "Float")
      case BasicType.Double => formatRowTensorStr(id, "Double")
      case BasicType.String => formatRowTensorStr(id, "String")
      case BasicType.ByteString => formatRowTensorStr(id, "ml.combust.mleap.tensor.ByteString") + ".map(i => com.google.protobuf.ByteString.copyFrom(i.bytes))"
      case _ => {
        assert(false)
        ""
      }
    }
  }

  private def encodeRowTensorIndicesValueToPb(inputType: BasicType, id: Int): String = {
    inputType match {
      case BasicType.Boolean => formatRowTensorIndicesStr(id, "Boolean")
      case BasicType.Byte => formatRowTensorIndicesStr(id, "Byte")
      case BasicType.Short => formatRowTensorIndicesStr(id, "Short")
      case BasicType.Int =>  formatRowTensorIndicesStr(id, "Int")
      case BasicType.Long => formatRowTensorIndicesStr(id, "Long")
      case BasicType.Float => formatRowTensorIndicesStr(id, "Float")
      case BasicType.Double => formatRowTensorIndicesStr(id, "Double")
      case BasicType.String => formatRowTensorIndicesStr(id, "String")
      case BasicType.ByteString => formatRowTensorIndicesStr(id, "ml.combust.mleap.tensor.ByteString")
      case _ => {
        assert(false)
        ""
      }
    }
  }

  // 根据模板生成protobuf描述文件
  def renderPbFile(tplFile: String = ""): String = {
    val src = if(tplFile.isEmpty) TemplateSource.fromURL(getClass.getResource("/predict.mustache"))
                   else TemplateSource.fromText("predict.mustache", tplFile)
    var id = 0

    //受限于scala和jvm，函数参数最大不超过127，method size最大64k
    val input = inputSchema().map(sf => {
      id = id + 1
      sf.dataType.shape match {
        case s: ScalarShape => Map[String,String]("type" -> encodeMleapTypeToPb(sf.dataType.base), "name" -> sf.name, "id" -> id.toString, "shape" -> "scalar")
        case l: ListShape => Map[String,String]("type" -> "repeated ".concat(encodeMleapTypeToPb(sf.dataType.base)), "name" -> sf.name, "id" -> id.toString, "shape" -> "list")
        // 对于tensor类型，转换成一维数组
        case t: TensorShape =>
          Map[String,String]("type" -> "repeated ".concat(encodeMleapTypeToPb(sf.dataType.base)),
                          "name" -> sf.name,
                          "id" -> id.toString,
                          "shape" -> "tensor"
                           )
        case _ =>
          assert(false)
          Map[String,String]()
      }
    }).sliding(MaxFieldNumer, MaxFieldNumer).toSeq

    var id2 = inputSchema().length
    val input_indices = input.map(slice => slice.filter(_("shape") == "tensor").map(item => {
      id2 = id2 + 1
      Map[String,String]("name" -> item("name"), "id" -> id2.toString)
    }))

    //output由于参数较少，暂时不考虑对参数进行拆分
    val output_indices = ArrayBuffer[Map[String,String]]()
    id = 0
    id2 = outputSchema().length
    val output = outputSchema().map(sf => {
      id = id + 1
      sf.dataType.shape match {
        case s: ScalarShape => Map[String,String]("type" -> encodeMleapTypeToPb(sf.dataType.base), "name" -> sf.name, "id" -> id.toString)
        case l: ListShape => Map[String,String]("type" -> "repeated ".concat(encodeMleapTypeToPb(sf.dataType.base)), "name" -> sf.name, "id" -> id.toString)
        // 对于tensor类型，转换成一维数组
        case t: TensorShape => {
          id2 = id2 + 1
          output_indices += Map[String,String]("name" -> sf.name, "id" -> id2.toString)
          Map[String,String]("type" -> "repeated ".concat(encodeMleapTypeToPb(sf.dataType.base)), "name" -> sf.name, "id" -> id.toString)
        }
        case _ => {
          assert(false)
          Map[String,String]()
        }
      }
    })

    if(input.length == 1) {
      val attributes = Map("input" -> input.head, "output" -> output, "input_indices"  -> input_indices.head, "output_indices" -> output_indices)

      engine.layout(src, attributes)
    }
    else {
      var mid = 0

      val pps = input.zip(input_indices).map(t => {
        mid = mid + 1
        Map("input" -> t._1, "input_indices" -> t._2, "mid" -> f"PP$mid")
      })

      val ni = pps.zipWithIndex.map(t => Map("type" -> t._1("mid"), "name" -> t._1("mid").toString.toLowerCase, "id" -> {t._2+1}))

      engine.layout(src, Map("pps" -> pps, "input" -> ni, "output" -> output, "output_indices" -> output_indices))
    }
  }

  // 根据模板生成grpc服务端文件
  def renderServiceFile(tplFile: String = ""): String = {
    val src = if(tplFile.isEmpty) TemplateSource.fromURL(getClass.getResource("/PredictServiceImpl.mustache"))
    else TemplateSource.fromText("PredictServiceImpl.mustache", tplFile)

    val SPLITED = inputSchema().length > MaxFieldNumer
    var mid = 0

    val inputs = inputSchema().sliding(MaxFieldNumer, MaxFieldNumer).toSeq.map(slice => {
      val inputLen = slice.length
      val comma = (i:Int) => if(i<inputLen) "," else ""
      var id = 0
      mid = mid + 1

      val input = slice.map(sf => {
        id = id + 1
        val name = if(SPLITED) f"pp$mid%s.orNull." + camelifyMethod( sf.name) else camelifyMethod( sf.name)

        sf.dataType.shape match {
          case s: ScalarShape => Map[String,String]("requestValue" -> decodePbValueToRow(sf.dataType.base,
            name), "comma" -> comma(id))
          case l: ListShape => Map[String,String]("requestValue" -> decodePbListValueToRow(sf.dataType.base, name), "comma" -> comma(id))
          // 对于tensor类型，转换成一维数组，区分dense和sparse两种类型
          case t: TensorShape =>
            val dimensions = sf.dataType.asInstanceOf[TensorType].dimensions.get.toList
            Map[String,String]("requestValue" -> decodePbTensorValueToRow(sf.dataType.base, name, dimensions), "comma" -> comma(id))
          case _ => {
            assert(false)
            Map[String,String]()
          }
        }
      })
      Map("input" -> input)
    })

    var id = 0
    val outputLen = outputSchema().length
    val tcount = outputSchema().count(sf => sf.dataType.shape.isTensor)
    val comma2 = (i:Int) => if(i<outputLen+tcount) "," else ""

    val tsId: ArrayBuffer[Int]  = ArrayBuffer[Int]()
    val output = outputSchema().map(sf => {
      id = id + 1
      sf.dataType.shape match {
        case s: ScalarShape => Map[String,String]("rowValue" -> encodeRowValueToPb(sf.dataType.base, id-1), "comma" -> comma2(id))
        case l: ListShape => Map[String,String]("rowValue" -> encodeRowListValueToPb(sf.dataType.base, id-1), "comma" -> comma2(id))
        // 对于tensor类型，转换成一维数组
        case t: TensorShape =>
          tsId.append(id-1)
          Map[String,String]("rowValue" -> encodeRowTensorValueToPb(sf.dataType.base, id-1), "comma" -> comma2(id))
        case _ => {
          assert(false)
          Map[String,String]()
        }
      }
    })

    val output_indices = outputSchema().filter(sf => sf.dataType.shape.isTensor).map(sf => Map[String,String]("rowValue" -> {
      id = id + 1
      encodeRowTensorIndicesValueToPb(sf.dataType.base, tsId.apply(id -  1 - outputLen))
    }, "comma" -> comma2(id) ))

    if(inputs.length == 1) {
      val attributes = Map("input" -> inputs.head("input"), "output" -> output, "output_indices" -> output_indices)
      engine.layout(src, attributes)
    }
    else {
      val attributes = Map("inputs" -> inputs, "output" -> output, "output_indices" -> output_indices)
      engine.layout(src, attributes)
    }
  }
}