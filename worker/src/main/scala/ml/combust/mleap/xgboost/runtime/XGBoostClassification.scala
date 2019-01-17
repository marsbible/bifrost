package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.runtime.frame.{MultiTransformer, Row, Transformer, UserDefinedBatchFunction}
import ml.dmlc.xgboost4j.scala.DMatrix
import ml.combust.mleap.core.util.VectorConverters._
import XgbConverters._

/**
  * Created by hollinwilkins on 9/16/17.
  */
case class XGBoostClassification(override val uid: String = Transformer.uniqueName("xgboost.classification"),
                                 override val shape: NodeShape,
                                 override val model: XGBoostClassificationModel) extends MultiTransformer {
  override val exec: UserDefinedFunction = {
    val rawPrediction = shape.getOutput("raw_prediction").map {
      _ => (data: DMatrix) => Some(model.predictRaw(data): Tensor[Double])
    }.getOrElse((_: DMatrix) => None)
    val probability = shape.getOutput("probability").map {
      _ => (data: DMatrix) => Some(model.predictProbabilities(data): Tensor[Double])
    }.getOrElse((_: DMatrix) => None)
    val prediction = (data: DMatrix) => Some(model.predict(data))
    val leafPrediction = shape.getOutput("leaf_prediction").map {
      _ => (data: DMatrix) => Some(model.predictLeaf(data))
    }.getOrElse((_: DMatrix) => None)
    val contribPrediction = shape.getOutput("contrib_prediction").map {
      _ => (data: DMatrix) => Some(model.predictContrib(data))
    }.getOrElse((_: DMatrix) => None)

    val rawPredictionBatch = shape.getOutput("raw_prediction").map {
      _ => (data: DMatrix) => Some(model.predictRawBatch(data).map(x => x: Tensor[Double]): Seq[Tensor[Double]])
    }.getOrElse((_: DMatrix) => None)
    val probabilityBatch = shape.getOutput("probability").map {
      _ => (data: DMatrix) => Some(model.predictProbabilitiesBatch(data).map(x => x: Tensor[Double]): Seq[Tensor[Double]])
    }.getOrElse((_: DMatrix) => None)
    val predictionBatch = (data: DMatrix) => Some(model.predictBatch(data): Seq[Double])
    val leafPredictionBatch = shape.getOutput("leaf_prediction").map {
      _ => (data: DMatrix) => Some(model.predictLeafBatch(data): Seq[Seq[Double]])
    }.getOrElse((_: DMatrix) => None)
    val contribPredictionBatch = shape.getOutput("contrib_prediction").map {
      _ => (data: DMatrix) => Some(model.predictContribBatch(data): Seq[Seq[Double]])
    }.getOrElse((_: DMatrix) => None)

    val all = Seq(rawPrediction, probability, prediction, leafPrediction, contribPrediction)
    val allBatch = Seq(rawPredictionBatch, probabilityBatch, predictionBatch, leafPredictionBatch, contribPredictionBatch)


    val f = (features: Tensor[Double]) => {
      val data = features.asXGB
      val rowData = all.map(_.apply(data)).filter(_.isDefined).map(_.get)
      Row(rowData: _*)
    }

    val bf = (features: Seq[Tensor[Double]]) => {
      val data = features.asXGB
      val rowsData = allBatch.map(_.apply(data)).filter(_.isDefined).map(_.get)

      val b = List.newBuilder[Row]
      var its = rowsData.map(_.iterator)
      while (its.nonEmpty) {
        its = its.filter(_.hasNext)
        b += Row(its.map(_.next): _*)
      }
      b.result
    }

    //UserDefinedFunction(f, outputSchema, inputSchema)
    UserDefinedBatchFunction(f, bf, outputSchema, inputSchema)
  }
}
