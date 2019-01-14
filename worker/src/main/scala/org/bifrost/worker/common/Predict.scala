package org.bifrost.worker.common

import ml.combust.bundle.BundleFile
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.MleapContext.defaultContext
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, RowTransformer, Transformer}
import resource._


trait Predict {
  val mleapPipeline: Transformer
  val outputFields: Array[String]
  val rowTransformer: RowTransformer

  def init(file: String): (Transformer, Array[String], RowTransformer) = {
    val tmp = (for(bf <- managed(BundleFile("jar:file:"+ file))) yield {
      (bf.loadMleapBundle().get.root, bf.readNote("output_fields").split(','))
    }).tried.get
    (tmp._1, tmp._2, tmp._1.transform(RowTransformer(tmp._1.inputSchema)).get)
  }

  def transform(frame: DefaultLeapFrame): DefaultLeapFrame = {
    //默认的transform
    val df = DefaultLeapFrame(mleapPipeline.outputSchema, frame.dataset.map(rowTransformer.transform))
    df.select(outputFields: _*).get
  }

  def inputSchema(): StructType = {
    mleapPipeline.inputSchema
  }

  def outputSchema(): StructType = {
    mleapPipeline.outputSchema
  }
}