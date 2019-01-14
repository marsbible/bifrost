package org.bifrost.jserver

import resource._
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.bundle.BundleFile
import resource.managed
import ml.combust.mleap.runtime._
import ml.combust.mleap.core.types.{StructField, _}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer, Transformer}
import ml.combust.mleap.tensor.DenseTensor

class XgbPredict {

  private val mleapPipeline: (Transformer, Array[String]) = (for(bf <- managed(BundleFile("jar:file:"+ getClass.getResource("/mnist.json").getPath.replace("mnist.json", "mleap-databricks-runtime-testkit.zip")))) yield {
    (bf.loadMleapBundle().get.root, bf.readNote("output_fields").split(','))
  }).tried.get
  val rowTransformer : RowTransformer = mleapPipeline._1.transform(RowTransformer(mleapPipeline._1.inputSchema)).get

  def predict(frame: DefaultLeapFrame): DefaultLeapFrame = {
    // transform the dataframe using our pipeline
    //val frame2 = mleapPipeline._1.transform(frame).get
    //println(mleapPipeline._1.outputSchema.fields)
    //frame2.select(mleapPipeline._2: _*).get

    val row = rowTransformer.transform(frame.dataset.head)
    DefaultLeapFrame(rowTransformer.outputSchema, Seq(row)).select(mleapPipeline._2: _*).get
  }
}

object XgbPredict {
  def main(args: Array[String]): Unit = {
    val predict = new XgbPredict

    val schema: StructType = StructType(
      StructField("loan_amount", ScalarType.Double),
      StructField("fico_score_group_fnl", ScalarType.String),
      StructField("dti", ScalarType.Double)
    ).get

    val dataset = Seq(Row(5000.0d, "0 - 450", 0.0d))
    val leapFrame = DefaultLeapFrame(schema, dataset)
    val result = predict.predict(leapFrame)

    result.show()
  }
}
