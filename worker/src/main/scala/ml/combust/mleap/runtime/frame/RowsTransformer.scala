package ml.combust.mleap.runtime.frame

import ml.combust.mleap.core.types.{StructField, StructType}
import ml.combust.mleap.runtime.frame.Row.RowSelector
import ml.combust.mleap.runtime.function.{Selector, UserDefinedFunction}

import scala.util.Try

/**
  *  RowsTransformer的主要目的是对于pipeline中个别支持批量处理的transformer给与优化，
  *  典型的对于通过jni调用c/c++代码且底层支持批量处理的库能有一定的性能提升。
  */
object RowsTransformer {
  def apply(schema: StructType): RowsTransformer = {
    RowsTransformer(schema,
      schema,
      schema.fields.length,
      schema.fields.indices,
      List(),
      Seq())
  }
}

case class RowsTransformer private (inputSchema: StructType,
                                   outputSchema: StructType,
                                   maxSize: Int,
                                   indices: Seq[Int],
                                   availableIndices: List[Int],
                                   transforms: Seq[Seq[ArrayRow] => Option[Seq[ArrayRow]]],
                                   shuffled: Boolean = false) extends FrameBuilder[RowsTransformer] {
  override def schema: StructType = outputSchema

  override def select(fieldNames: String *): Try[RowsTransformer] = {
    for (indices <- outputSchema.indicesOf(fieldNames: _*);
         schema2 <- outputSchema.selectIndices(indices: _*)) yield {
      val s = Set(indices: _*)
      val newIndices = indices.map(this.indices)
      val dropped = this.indices.zipWithIndex.filterNot {
        case (_, i) => s.contains(i)
      }.map(_._1)
      val newAvailableIndices = List(dropped: _*) ::: availableIndices

      copy(outputSchema = schema2,
        indices = newIndices,
        availableIndices = newAvailableIndices,
        shuffled = true)
    }
  }

  def udfValues(rows: Seq[Row], selectors: RowSelector *)(udf: UserDefinedBatchFunction): Seq[Any] = {
    udf.inputs.length match {
      case 0 =>
        udf.bf.asInstanceOf[() => Seq[Any]]()
      case 1 =>
        udf.bf.asInstanceOf[Seq[Any] => Seq[Any]](rows.map(selectors.head))
      case 2 =>
        udf.bf.asInstanceOf[(Seq[Any], Seq[Any]) => Seq[Any]](rows.map(selectors.head), rows.map(selectors(1)))
      case 3 =>
        udf.bf.asInstanceOf[(Seq[Any], Seq[Any], Seq[Any]) => Seq[Any]](rows.map(selectors.head), rows.map(selectors(1)), rows.map(selectors(2)))
      case 4 =>
        udf.bf.asInstanceOf[(Seq[Any], Seq[Any], Seq[Any], Seq[Any]) => Seq[Any]](rows.map(selectors.head), rows.map(selectors(1)), rows.map(selectors(2)), rows.map(selectors(3)))
      case 5 =>
        udf.bf.asInstanceOf[(Seq[Any], Seq[Any], Seq[Any], Seq[Any], Seq[Any]) => Seq[Any]](rows.map(selectors.head), rows.map(selectors(1)), rows.map(selectors(2)), rows.map(selectors(3)), rows.map(selectors(4)))
    }
  }

  override def withColumn(name: String, selectors: Selector *)
                         (udf: UserDefinedFunction): Try[RowsTransformer] = {
    ensureAvailableIndices(1).withColumnUnsafe(name, selectors: _*)(udf)
  }

  def withColumnUnsafe(name: String, selectors: Selector *)
                      (udf: UserDefinedFunction): Try[RowsTransformer] = {
    val index :: newAvailableIndices = availableIndices

    IndexedRowUtil.createRowSelectors(outputSchema, this.indices, selectors: _*)(udf).flatMap {
      rowSelectors =>
        outputSchema.withField(name, udf.outputTypes.head).map {
          schema2 =>
            val transform = (rows: Seq[ArrayRow]) => {
              udf match {
                //对于支持batch运算的
                case _: UserDefinedBatchFunction => rows.view.zip(udfValues(rows, rowSelectors: _*)(udf.asInstanceOf[UserDefinedBatchFunction])).foreach { case (row, v) => row.set(index, v)}
                case _ => rows.foreach(row => row.set(index, row.udfValue(rowSelectors: _*)(udf)))
              }
              Some(rows)
            }
            copy(outputSchema = schema2,
              indices = indices :+ index,
              availableIndices = newAvailableIndices,
              transforms = transforms :+ transform)
        }
    }
  }

  override def withColumns(names: Seq[String], selectors: Selector *)
                          (udf: UserDefinedFunction): Try[RowsTransformer] = {
    ensureAvailableIndices(names.length).withColumnsUnsafe(names, selectors: _*)(udf)
  }

  def withColumnsUnsafe(names: Seq[String], selectors: Selector *)
                       (udf: UserDefinedFunction): Try[RowsTransformer] = {
    val (indices, newAvailableIndices) = names.foldLeft((Seq[Int](), availableIndices)) {
      case ((is, ais), _) =>
        val i :: ai = ais
        (is :+ i, ai)
    }


    val fields = names.zip(udf.outputTypes).map {
      case (name, dt) => StructField(name, dt)
    }

    IndexedRowUtil.createRowSelectors(outputSchema, this.indices, selectors: _*)(udf).flatMap {
      rowSelectors =>
        outputSchema.withFields(fields).map {
          schema2 =>
            val transform = {
              rows: Seq[ArrayRow] =>
                val outputs = udf match {
                  case _: UserDefinedBatchFunction => udfValues(rows, rowSelectors: _*)(udf.asInstanceOf[UserDefinedBatchFunction])
                  case _ =>  rows.map(row => row.udfValue(rowSelectors: _*)(udf))
                }

                outputs.view.map  {
                  case r: ArrayRow => r.iterator.toSeq
                  case x => x.asInstanceOf[Product].productIterator.toSeq
                }.zip(rows).foreach {
                  case (value, row) => indices.zip(value).foreach {
                    case (i, v) => row.set(i, v)
                  }
                }

                Some(rows)
            }

            copy(outputSchema = schema2,
              indices = this.indices ++ indices,
              availableIndices = newAvailableIndices,
              transforms = transforms :+ transform)
        }
    }
  }

  override def drop(names: String *): Try[RowsTransformer] = {
    for (indices <- outputSchema.indicesOf(names: _*);
         schema2 <- outputSchema.dropIndices(indices: _*)) yield {
      val s = Set(indices: _*)
      val newIndices = this.indices.zipWithIndex.filterNot {
        case (_, i) => s.contains(i)
      }.map(_._1)
      val dropped = this.indices.zipWithIndex.filter {
        case (_, i) => s.contains(i)
      }.map(_._1)
      val newAvailableIndices = List(dropped: _*) ::: availableIndices

      copy(outputSchema = schema2,
        indices = newIndices,
        availableIndices = newAvailableIndices,
        shuffled = true)
    }
  }

  override def filter(selectors: Selector *)
                     (udf: UserDefinedFunction): Try[RowsTransformer] = {
    IndexedRowUtil.createRowSelectors(outputSchema, indices, selectors: _*)(udf).map {
      rowSelectors =>
        val transform = (rows: Seq[ArrayRow]) => {
          val outputs = udf match {
            case _: UserDefinedBatchFunction => udfValues(rows, rowSelectors: _*)(udf.asInstanceOf[UserDefinedBatchFunction])
            case _ =>  rows.map(row => row.udfValue(rowSelectors: _*)(udf))
          }

          Some(rows.view.zip(outputs).filter(_._2.asInstanceOf[Boolean]).map(_._1).force)
        }

        copy(transforms = transforms :+ transform)
    }
  }

  def ensureAvailableIndices(numAvailable: Int): RowsTransformer = {
    if(availableIndices.size < numAvailable) {
      val diff = numAvailable - availableIndices.size
      val newMaxSize = maxSize + diff
      val newAvailableIndices = availableIndices ::: List(maxSize until newMaxSize: _*)

      copy(maxSize = newMaxSize,
        availableIndices = newAvailableIndices)
    } else {
      this
    }
  }

  def transform(rows: Seq[Row]): Seq[Row] = {
    transformOption(rows).orNull
  }

  /** Transform an input row with the predetermined schema.
    *
    * @param rows row to transform
    * @return transformed row, or None if filtered
    */
  def transformOption(rows: Seq[Row]): Option[Seq[ArrayRow]] = {
    val arrRows = rows.map(row => {
      val arr = new Array[Any](maxSize)
      row.toArray.copyToArray(arr)
      ArrayRow(arr)
    })

    val r = transforms.foldLeft(Option(arrRows)) {
      (r, transform) => r.flatMap(transform)
    }

    if(shuffled) {
      r.map {
        rows =>
          rows.map(row => ArrayRow(indices.map(row.getRaw)))
      }
    } else {
      r
    }
  }
}