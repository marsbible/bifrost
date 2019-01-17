package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.tensor.Tensor
import ml.dmlc.xgboost4j.LabeledPoint
import ml.dmlc.xgboost4j.scala.DMatrix
import org.apache.spark.ml.linalg.Vector

trait XgbConverters {
  implicit class VectorOps(vector: Vector) {
    def asXGB: DMatrix = {
      new DMatrix(Iterator(new LabeledPoint(0.0f, null, vector.toDense.values.map(_.toFloat))))
    }
  }

  implicit class VectorsOps(vectors: Seq[Vector]) {
    def asXGB: DMatrix = {
      new DMatrix(vectors.view.map(vector => new LabeledPoint(0.0f, null, vector.toDense.values.map(_.toFloat))).iterator)
    }
  }

  implicit class DoubleTensorOps(tensor: Tensor[Double]) {
    def asXGB: DMatrix = {
      new DMatrix(Iterator(new LabeledPoint(0.0f, null, tensor.toDense.rawValues.map(_.toFloat))))
    }
  }

  implicit class DoubleTensorsOps(tensors: Seq[Tensor[Double]]) {
    def asXGB: DMatrix = {
      new DMatrix(tensors.view.map(tensor => new LabeledPoint(0.0f, null,
                  tensor.toDense.rawValues.map(_.toFloat))).iterator)
    }
  }
}

object XgbConverters extends XgbConverters
