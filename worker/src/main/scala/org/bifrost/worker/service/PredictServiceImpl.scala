package org.bifrost.worker.service

import akka.event.LoggingAdapter
import akka.grpc.GrpcServiceException
import akka.stream.Materializer
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}
import ml.combust.mleap.tensor.SparseTensor
import org.bifrost.worker.Main
import org.bifrost.worker.common._
import org.bifrost.worker.grpc._

import scala.concurrent.Future

/**
  模板
*/
class PredictServiceImpl(main: Main, modelPath: String) extends PredictService with Predict{
  private implicit val mat: Materializer = main.mat
  private val logger: LoggingAdapter = main.logger
  val (mleapPipeline, outputFields, rowTransformer) = init(modelPath)


  def fromRequest(request: PredictRequest): DefaultLeapFrame = {
    val row = Row(
            request.loanAmount,
            request.ficoScoreGroupFnl,
            request.dti
            )
            DefaultLeapFrame(inputSchema(), Seq(row))
  }

  def fromRequests(requests: PredictRequests): DefaultLeapFrame = {
    val rows = requests.request.map(request => {
      Row(
        request.loanAmount,
        request.ficoScoreGroupFnl,
        request.dti
        )
        })

    DefaultLeapFrame(inputSchema(), rows)
  }

  def toReply(df: DefaultLeapFrame): PredictReply = {
    val row = df.dataset.head

    PredictReply(
      row.getTensor[Double](0).rawValues.toSeq,
      row.getTensor[Double](1).rawValues.toSeq,
      row.getDouble(2),
      if(row.getTensor[Double](0).isSparse) row.getTensor[Double](0).asInstanceOf[SparseTensor[Double]].indices.map(i => DataIndex(i)) else Seq.empty,
      if(row.getTensor[Double](1).isSparse) row.getTensor[Double](1).asInstanceOf[SparseTensor[Double]].indices.map(i => DataIndex(i)) else Seq.empty
      )
  }

  def toReplies(df: DefaultLeapFrame): PredictReplies = {
    val replies = df.dataset.map(row => {
      PredictReply(
         row.getTensor[Double](0).rawValues.toSeq,
         row.getTensor[Double](1).rawValues.toSeq,
         row.getDouble(2),
         if(row.getTensor[Double](0).isSparse) row.getTensor[Double](0).asInstanceOf[SparseTensor[Double]].indices.map(i => DataIndex(i)) else Seq.empty,
         if(row.getTensor[Double](1).isSparse) row.getTensor[Double](1).asInstanceOf[SparseTensor[Double]].indices.map(i => DataIndex(i)) else Seq.empty
         )
    })

    PredictReplies(replies)
  }

  override def predict(in: PredictRequest): Future[PredictReply] = {
    val start = System.currentTimeMillis()
    try {
      val df = transform(fromRequest(in))
      val reply = toReply(df)

      val end = System.currentTimeMillis()
      logger.info("predict time cost {} ms", end - start)

      Future.successful(reply)
    } catch {
      case t: Throwable => Future.failed(new GrpcServiceException(io.grpc.Status.INVALID_ARGUMENT.withDescription(t.getMessage)))
    }
  }

  override def predictMulti(in: PredictRequests): Future[PredictReplies] = {
    val start = System.currentTimeMillis()
    try {
      val df = transform(fromRequests(in))
      val replies = toReplies(df)

      val end = System.currentTimeMillis()
      logger.info("predictMulti time cost {} ms", end - start)

      Future.successful(replies)
    }catch {
      case t: Throwable => Future.failed(new GrpcServiceException(io.grpc.Status.INVALID_ARGUMENT.withDescription(t.getMessage)))
    }
  }
}