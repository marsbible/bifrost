package org.bifrost.jserver

import java.io.File
import java.nio.file.{Files, StandardCopyOption}

import org.apache.spark.ml.feature.OneHotEncoderEstimator

import ml.combust.bundle.BundleFile
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.databricks.spark.avro._
import ml.bundle.Tensor
import ml.combust.mleap.spark.SparkSupport._
import ml.combust.mleap.runtime.MleapSupport._
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassifier
import org.apache.spark.ml.Pipeline
import resource.managed

class XgbTrain {
  private val spark = SparkSession.builder.appName("mleap").master("local[*]").getOrCreate

  private val xgboostParams: Map[String, Any] = Map(
    "eta" -> 0.3,
    "max_depth" -> 2,
    "objective" -> "binary:logistic",
    "early_stopping_rounds" ->2,
    "num_round" -> 15,
    "nworkers" -> 2
  )

  def loadData(): (DataFrame, Option[DataFrame]) = {
    val sqlContext  = spark.sqlContext

    // Create a temporary file and copy the contents of the resource avro to it
    val path = Files.createTempFile("mleap-databricks-runtime-testkit", ".avro")
    Files.copy(getClass.getResource("/lending_club_sample.avro").openStream(),
      path,
      StandardCopyOption.REPLACE_EXISTING)

    val sampleData = sqlContext.read.avro(path.toString)
    sampleData.printSchema()
    sampleData.show()

    (sampleData, None)
  }

  def transform(dataset: DataFrame, test: Option[DataFrame]): Int = {
    val stringIndexer = new StringIndexer().
      setInputCol("fico_score_group_fnl").
      setOutputCol("fico_index")

    val featureAssembler = new VectorAssembler().
      setInputCols(Array(stringIndexer.getOutputCol, "dti", "loan_amount")).
      setOutputCol("features")

    val logisticRegression = new XGBoostClassifier(xgboostParams).
      setFeaturesCol("features").
      setLabelCol("approved").
      setPredictionCol("prediction")


    val pipeline = new Pipeline().setStages(Array(stringIndexer, featureAssembler, logisticRegression))

    val model = pipeline.fit(dataset)
    
    {
      implicit val sbc = SparkBundleContext().withDataset(model.transform(dataset))

      for(bf <- managed(BundleFile("jar:file:"+ getClass.getResource("/mnist.json").getPath.replace("mnist.json", "mleap-databricks-runtime-testkit.zip")))) {
        model.writeBundle.save(bf)(sbc).get
        bf.writeNote("output_fields", "rawPrediction,probability,prediction")
      }
    }

    0
  }
}

object XgbTrain {
  def main(args:Array[String]): Unit = {
    val x = new XgbTrain
    val (df, dt) = x.loadData()

    x.transform(df, dt)
  }
}
