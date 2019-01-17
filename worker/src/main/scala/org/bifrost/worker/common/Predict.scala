package org.bifrost.worker.common

import ml.combust.bundle.BundleFile
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.MleapContext.defaultContext
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, RowTransformer, RowsTransformer, Transformer}
import resource._


trait Predict {
  val mleapPipeline: Transformer
  val outputFields: Array[String]
  val rowTransformer: RowTransformer
  val rowsTransformer: RowsTransformer
  val batchEnabled: Boolean

  def init(file: String): (Transformer, Array[String], RowTransformer, RowsTransformer) = {
    val tmp = (for(bf <- managed(BundleFile("jar:file:"+ file))) yield {
      (bf.loadMleapBundle().get.root, bf.readNote("output_fields").split(','))
    }).tried.get
    (tmp._1, tmp._2, tmp._1.transform(RowTransformer(tmp._1.inputSchema)).get, tmp._1.transform(RowsTransformer(tmp._1.inputSchema)).get)
  }

  def transform(frame: DefaultLeapFrame): DefaultLeapFrame = {
    val df = DefaultLeapFrame(rowTransformer.outputSchema, frame.dataset.map(rowTransformer.transform).filter(_ != null))
    df.select(outputFields: _*).get
  }

  def transforms(frame: DefaultLeapFrame): DefaultLeapFrame = {
    //默认的transforms
    val df = if(batchEnabled) DefaultLeapFrame(rowTransformer.outputSchema, rowsTransformer.transform(frame.dataset))
                else DefaultLeapFrame(rowTransformer.outputSchema, frame.dataset.map(rowTransformer.transform).filter(_ != null))
    df.select(outputFields: _*).get
  }

  def inputSchema(): StructType = {
    mleapPipeline.inputSchema
  }

  def outputSchema(): StructType = {
    mleapPipeline.outputSchema
  }
}