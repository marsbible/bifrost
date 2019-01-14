package org.bifrost.jserver

import resource._
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.mleap.runtime.MleapContext.defaultContext
import java.io.InputStream

import ml.combust.mleap.runtime.serialization.FrameReader
import ml.combust.bundle.BundleFile
import ml.combust.bundle.dsl.Value
import ml.combust.mleap.core.types.TensorType
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer, Transformer}
import ml.combust.mleap.runtime.function.{Selector, UserDefinedFunction}
import ml.combust.mleap.runtime.transformer.feature.VectorAssembler
import ml.combust.mleap.tensor.{DenseTensor, Tensor}


class MnistPredict {
  private val mleapPipeline: (Transformer, Array[String]) = (for(bf <- managed(BundleFile("jar:file:"+ getClass.getResource("/mnist.json").getPath.replace("mnist.json", "mnist.model.rf.zip")))) yield {
    (bf.loadMleapBundle().get.root, bf.readNote("output_fields").split(','))
  }).tried.get

  def predict(frame: DefaultLeapFrame): DefaultLeapFrame = {
    // transform the dataframe using our pipeline
    val frame2 = mleapPipeline._1.transform(frame).get
    frame2.select(mleapPipeline._2: _*).get

    /*
    val rowTransformer : RowTransformer = mleapPipeline._1.transform(RowTransformer(mleapPipeline._1.inputSchema)).get
    val row = rowTransformer.transform(frame.dataset.head)
    DefaultLeapFrame(rowTransformer.outputSchema, Seq(row))
    */
  }
}

object MnistPredict {
  def main(args:Array[String]): Unit = {
    val predict = new MnistPredict

    val stream : InputStream = getClass.getResourceAsStream("/mnist.json")

    val s = scala.io.Source.fromInputStream(stream).mkString

    val bytes = s.getBytes("UTF-8")
    val frame = FrameReader("ml.combust.mleap.json").fromBytes(bytes)

    val result = predict.predict(frame.get)
    result.show()
    //result.select("probability").get.collect().foreach((i: Row) => i.getAs[DenseTensor[Double]](0).rawValuesIterator.foreach((d:Double) => println(d)))
  }
}
