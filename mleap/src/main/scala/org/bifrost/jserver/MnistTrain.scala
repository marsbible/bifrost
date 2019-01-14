package org.bifrost.jserver

import resource._
import ml.combust.bundle.BundleFile
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{Binarizer, IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.mleap.SparkUtil
import ml.combust.mleap.spark.SparkSupport._
import org.apache.spark.ml.linalg.{SparseVector, Vectors}
import org.apache.spark.sql.functions.udf
import org.bifrost.jserver.MnistPredict.getClass


class MnistTrain {
  private val spark = SparkSession.builder.appName("mleap").master("local[*]").getOrCreate

  def loadData(): (DataFrame,DataFrame) = {
    val datasetPath = getClass.getResource("/mnist_train.csv").getPath
    var dataset = spark.sqlContext.read.format("com.databricks.spark.csv").
      option("header", "true").
      option("inferSchema", "true").
      load(datasetPath)

    val testDatasetPath = getClass.getResource("/mnist_test.csv").getPath
    var test = spark.sqlContext.read.format("com.databricks.spark.csv").
      option("inferSchema", "true").
      option("header", "true").
      load(testDatasetPath)

    (dataset,test)
  }

  def transform(dataset: DataFrame, test: DataFrame): Int = {
    val predictionCol = "label"
    val labels = Seq("0","1","2","3","4","5","6","7","8","9")
    val pixelFeatures = (0 until 784).map(x => s"x$x").toArray

    val layers = Array[Int](pixelFeatures.length, 784, 800, labels.length)

    val vector_assembler = new VectorAssembler()
      .setInputCols(pixelFeatures)
      .setOutputCol("features")

    //val dataset2 = vector_assembler.transform(dataset)
    val dataset2 = dataset

    //此后的流程是训练和预测公用的流程
    val stringIndexer = { new StringIndexer()
      .setInputCol(predictionCol)
      .setOutputCol("label_index")
      .fit(dataset2)
    }

    val binarizer = new Binarizer()
      .setInputCol(vector_assembler.getOutputCol)
      .setThreshold(127.5)
      .setOutputCol("binarized_features")

    val pca = new PCA().
      setInputCol(binarizer.getOutputCol).
      setOutputCol("pcaFeatures").
      setK(10)


    val featurePipeline = new Pipeline().setStages(Array( vector_assembler, stringIndexer, binarizer, pca))

    // Transform the raw data with the feature pipeline and persist it
    val featureModel = featurePipeline.fit(dataset2)

    val datasetWithFeatures = featureModel.transform(dataset2)

    // Select only the data needed for training and persist it
    val datasetPcaFeaturesOnly = datasetWithFeatures.select(stringIndexer.getOutputCol, pca.getOutputCol)
    val datasetPcaFeaturesOnlyPersisted = datasetPcaFeaturesOnly.persist()

    val rf = new RandomForestClassifier().
      setFeaturesCol(pca.getOutputCol).
      setLabelCol(stringIndexer.getOutputCol).
      setPredictionCol("prediction").
      setProbabilityCol("probability").
      setRawPredictionCol("raw_prediction")

    val rfModel = rf.fit(datasetPcaFeaturesOnlyPersisted)

    val pipeline = SparkUtil.createPipelineModel(uid = "pipeline", Array(featureModel, rfModel))
    val sbc = SparkBundleContext().withDataset(rfModel.transform(datasetWithFeatures))

    for(bf <- managed(BundleFile("jar:file:"+ getClass.getResource("/mnist.json").getPath.replace("mnist.json", "mnist.model.rf.zip")))) {
      pipeline.writeBundle.save(bf)(sbc).get
      bf.writeNote("output_fields", "raw_prediction,probability,prediction")
    }

    0
  }
}

object MnistTrain {
  def main(args:Array[String]): Unit = {
    val train = new MnistTrain

    val (ds,ts) = train.loadData()

    train.transform(ds, ts)
  }
}