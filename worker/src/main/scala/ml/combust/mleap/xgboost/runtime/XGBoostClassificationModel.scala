package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.classification.ProbabilisticClassificationModel
import ml.combust.mleap.core.types.{ListType, ScalarType, StructType, TensorType}
import ml.dmlc.xgboost4j.scala.{Booster, DMatrix}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import XgbConverters._

trait XGBoostClassificationModelBase extends ProbabilisticClassificationModel {
  def booster: Booster
  def treeLimit: Int

  override def predict(features: Vector): Double = predict(features.asXGB)
  def predict(data: DMatrix): Double
  def predictBatch(data: DMatrix): Seq[Double]

  override def predictRaw(features: Vector): Vector = predictRaw(features.asXGB)
  def predictRaw(data: DMatrix): Vector
  def predictRawBatch(data: DMatrix): Seq[Vector]

  override def predictProbabilities(features: Vector): Vector = predictProbabilities(features.asXGB)
  def predictProbabilities(data: DMatrix): Vector
  def predictProbabilitiesBatch(data: DMatrix): Seq[Vector]

  def predictLeaf(features: Vector): Seq[Double] = predictLeaf(features.asXGB)
  def predictLeaf(data: DMatrix): Seq[Double] = booster.predictLeaf(data, treeLimit = treeLimit).head.map(_.toDouble)
  def predictLeafBatch(data: DMatrix): Seq[Seq[Double]] = booster.predictLeaf(data, treeLimit = treeLimit).map(row => row.map(_.toDouble).toSeq)

  def predictContrib(features: Vector): Seq[Double] = predictContrib(features.asXGB)
  def predictContrib(data: DMatrix): Seq[Double] = booster.predictContrib(data, treeLimit = treeLimit).head.map(_.toDouble)
  def predictContribBatch(data: DMatrix): Seq[Seq[Double]] = booster.predictContrib(data, treeLimit = treeLimit).map(row => row.map(_.toDouble).toSeq)
}

case class XGBoostBinaryClassificationModel(override val booster: Booster,
                                            override val numFeatures: Int,
                                            override val treeLimit: Int) extends XGBoostClassificationModelBase {
  override val numClasses: Int = 2

  def predict(data: DMatrix): Double = {
    Math.round(booster.predict(data).head(0))
  }

  def predictBatch(data: DMatrix): Seq[Double] = {
    booster.predict(data).map(row => Math.round(row(0)).toDouble).toSeq
  }

  def predictProbabilities(data: DMatrix): Vector = {
    val m = booster.predict(data, outPutMargin = false, treeLimit = treeLimit).head(0)
    Vectors.dense(1 - m, m)
  }

  def predictProbabilitiesBatch(data: DMatrix): Seq[Vector] = {
    booster.predict(data, outPutMargin = false, treeLimit = treeLimit).
              map(row => Vectors.dense(1 - row(0), row(0)))
  }

  def predictRaw(data: DMatrix): Vector = {
    val m = booster.predict(data, outPutMargin = true, treeLimit = treeLimit).head(0)
    Vectors.dense(1 - m, m)
  }

  def predictRawBatch(data: DMatrix): Seq[Vector] = {
    booster.predict(data, outPutMargin = true, treeLimit = treeLimit).
      map(row => Vectors.dense(1 - row(0), row(0)))
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    throw new Exception("XGBoost Classification model does not support \'rawToProbabilityInPlace\'")
  }
}

case class XGBoostMultinomialClassificationModel(override val booster: Booster,
                                                 override val numClasses: Int,
                                                 override val numFeatures: Int,
                                                 override val treeLimit: Int) extends XGBoostClassificationModelBase {

  override def predict(data: DMatrix): Double = {
    probabilityToPrediction(predictProbabilities(data))
  }

  def predictBatch(data: DMatrix): Seq[Double] = {
    predictProbabilitiesBatch(data).map(probabilityToPrediction)
  }

  def predictProbabilities(data: DMatrix): Vector = {
    Vectors.dense(booster.predict(data, outPutMargin = false, treeLimit = treeLimit).head.map(_.toDouble))
  }

  def predictProbabilitiesBatch(data: DMatrix): Seq[Vector] = {
    booster.predict(data, outPutMargin = false, treeLimit = treeLimit).map(row => Vectors.dense(row.map(_.toDouble)))
  }

  def predictRaw(data: DMatrix): Vector = {
    Vectors.dense(booster.predict(data, outPutMargin = true, treeLimit = treeLimit).head.map(_.toDouble))
  }

  def predictRawBatch(data: DMatrix): Seq[Vector] = {
    booster.predict(data, outPutMargin = true, treeLimit = treeLimit).map(row => Vectors.dense(row.map(_.toDouble)))
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    throw new Exception("XGBoost Classification model does not support \'rawToProbabilityInPlace\'")
  }
}

case class XGBoostClassificationModel(impl: XGBoostClassificationModelBase) extends ProbabilisticClassificationModel {
  override val numClasses: Int = impl.numClasses
  override val numFeatures: Int = impl.numFeatures
  def treeLimit: Int = impl.treeLimit

  def booster: Booster = impl.booster

  def binaryClassificationModel: XGBoostBinaryClassificationModel = impl.asInstanceOf[XGBoostBinaryClassificationModel]
  def multinomialClassificationModel: XGBoostMultinomialClassificationModel = impl.asInstanceOf[XGBoostMultinomialClassificationModel]

  def predict(data: DMatrix): Double = impl.predict(data)
  def predictBatch(data: DMatrix): Seq[Double] = impl.predictBatch(data)

  def predictLeaf(features: Vector): Seq[Double] = impl.predictLeaf(features)
  def predictLeaf(data: DMatrix): Seq[Double] = impl.predictLeaf(data)
  def predictLeafBatch(data: DMatrix): Seq[Seq[Double]] = impl.predictLeafBatch(data)

  def predictContrib(features: Vector): Seq[Double] = impl.predictContrib(features)
  def predictContrib(data: DMatrix): Seq[Double] = impl.predictContrib(data)
  def predictContribBatch(data: DMatrix): Seq[Seq[Double]] = impl.predictContribBatch(data)

  override def predictProbabilities(features: Vector): Vector = impl.predictProbabilities(features)
  def predictProbabilities(data: DMatrix): Vector = impl.predictProbabilities(data)
  def predictProbabilitiesBatch(data: DMatrix): Seq[Vector] = impl.predictProbabilitiesBatch(data)

  override def predictRaw(features: Vector): Vector = impl.predictRaw(features)
  def predictRaw(data: DMatrix): Vector = impl.predictRaw(data)
  def predictRawBatch(data: DMatrix): Seq[Vector] = impl.predictRawBatch(data)

  override def rawToProbabilityInPlace(raw: Vector): Vector = impl.rawToProbabilityInPlace(raw)

  override def outputSchema: StructType = StructType("raw_prediction" -> TensorType.Double(numClasses),
    "probability" -> TensorType.Double(numClasses),
    "prediction" -> ScalarType.Double.nonNullable,
    "leaf_prediction" -> ListType.Double,
    "contrib_prediction" -> ListType.Double).get
}
