package org.bifrost.worker.service

import akka.event.LoggingAdapter
import akka.grpc.GrpcServiceException
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.stream.Materializer
import com.sksamuel.elastic4s.http.search.SearchResponse
import com.sksamuel.elastic4s.http.{RequestFailure, RequestSuccess}
import org.apache.hadoop.hbase.{CellUtil, TableName}
import org.apache.hadoop.hbase.client.{Get, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.bifrost.worker.Main
import org.bifrost.worker.common.{BifrostException, GrpcFunction}
import org.bifrost.worker.grpc._
import scalapb.json4s.JsonFormat
import org.mongodb.scala.model.Filters._

import scala.concurrent.{ExecutionContext, Future, Promise}


/**
  * 自定义服务demo，包含redis,hbase,mongodb,solr,es,mysql,http的demo方法
  * 建议按照下面的模板编写，调优时可以按照不同客户端配置不同的dispatcher优化线程池
  * 将用户输入的特定的参数经过程序处理后转换成PredictService需要的参数，然后进行预测返回结果
* */
class WorkerServiceImpl(main: Main, predictService: PredictServiceImpl) extends WorkerService with GrpcFunction{
  private implicit val mat: Materializer = main.mat
  private val logger: LoggingAdapter = main.logger
  implicit val ec: ExecutionContext = main.ec

  //dummy方法，只是为了让程序编译通过，不要调用
  def dummy(in: WorkerRequest): Future[PredictRequest] = {
    val predictRequest = PredictRequest.defaultInstance

    Future.successful(predictRequest)
  }

  def dummy(in: WorkerRequests): Future[PredictRequests] = {
    val predictRequests = PredictRequests(in.request.map(x => PredictRequest.defaultInstance))

    Future.successful(predictRequests)
  }

  /********** xxDemo仅供参考和演示，用户可以自定义输入变换方法 ****************/
  /*
  def redisDemo(in: WorkerRequest): Future[PredictRequest] = {
    val client = main.getRedisClient("redis")
    implicit val ec: ExecutionContext = main.getExecutionContext("redis")

    client.get[String](in.xid)
      .map(v => {
        JsonFormat.fromJsonString[PredictRequest](v.get)
      })
  }

  def hbaseDemo(in: WorkerRequest): Future[PredictRequest] = {
    implicit val ec: ExecutionContext = main.getExecutionContext("hbase")
    Future {
      val client = main.getHbaseClient("hbase")
      val table = client.getTable(TableName.valueOf("bifrost", "xgb-test"))
      val args = new Array[String](3)

      try {
        var get = new Get(Bytes.toBytes(in.xid))
        get.addFamily(Bytes.toBytes("features"))
        var result = table.get(get)

        for (cell <- result.rawCells()) {
          val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
          val col_value = CellUtil.cloneValue(cell)

          col_name match {
            case "loan_amount" => args(0) = Bytes.toString(col_value)
            case "fico_score_group_fnl" => args(1) = Bytes.toString(col_value)
            case "dti" => args(2) = Bytes.toString(col_value)
            case _ =>
          }
        }

        /* Scan example
      var scan = table.getScanner(new Scan())
      scan.asScala.foreach(result => {
        printRow(result)
      })
      */
        PredictRequest(args(0).toDouble, args(1), args(2).toDouble)
      }
      finally {
        table.close()
      }
    }
  }

  def mongoDemo(in: WorkerRequest): Future[PredictRequest] = {
    val client = main.getMongoClient("mongo")

    client.getCollection("xgb-test").find(equal("xid", in.xid)).first().toFuture()
      .map(doc => {
        PredictRequest(doc.getDouble("loan_amount"),
          doc.getString("fico_score_group_fnl"), doc.getDouble("dti"))
      })
  }

  def solrDemo(in: WorkerRequest): Future[PredictRequest] = {
    implicit val ec: ExecutionContext = main.getExecutionContext("solr")

    Future {
      val client = main.getSolrClient("solr")

      import org.apache.solr.client.solrj.SolrQuery
      val query = new SolrQuery("*:*")
      query.addFilterQuery("id:" + in.xid)

      val response = client.query("bifrost", query)
      if(response.getResults.isEmpty) {
        throw new BifrostException(io.grpc.Status.NOT_FOUND, "Invalid query xid: " + in.xid)
      }
      val dl = response.getResults.get(0)

      PredictRequest(dl.get("loan_amount").asInstanceOf[Double], dl.get("fico_score_group_fnl").asInstanceOf[String]
        , dl.get("dti").asInstanceOf[Double])
    }
  }

  def esDemo(in: WorkerRequest): Future[PredictRequest] = {
    val client = main.getEsClient("es")
    import com.sksamuel.elastic4s.http.ElasticDsl._

    client.execute {
      println(Thread.currentThread().getName)
      searchWithType("bifrost" / "v1").query(termQuery("xid", in.xid))
    }.map {
      case failure: RequestFailure => throw new BifrostException(io.grpc.Status.INVALID_ARGUMENT, failure.error.reason)
      case results: RequestSuccess[SearchResponse] =>
        val docs = results.result.hits.hits.toList
        if(docs.isEmpty) throw new BifrostException(io.grpc.Status.NOT_FOUND, "Invalid query xid: " + in.xid)
        val doc = docs.head.sourceAsMap
        PredictRequest(doc("loan_amount").asInstanceOf[Double], doc("fico_score_group_fnl").asInstanceOf[String], doc("dti").asInstanceOf[Double])
      //case results: RequestSuccess[_] => throw new BifrostException(io.grpc.Status.INVALID_ARGUMENT, "No documents found")
    }
  }

  def sqlDemo(in: WorkerRequest): Future[PredictRequest] = {
    val client = main.getSqlClient("mysql")
    import slick.jdbc.MySQLProfile.api._

    val action = sql"""select loan_amount,fico_score_group_fnl,dti from `xgb-test` where xid=${in.xid}""".as[(Double,String,Double)]

    client.run(action).map(results => {
      if(results.isEmpty) throw new BifrostException(io.grpc.Status.NOT_FOUND, "Invalid query xid: " + in.xid)
      val rec = results.head
      PredictRequest(rec._1, rec._2, rec._3)
    })
  }
  */
  /********************* xxDemo结束 ******************/


  def partial(): PartialFunction[HttpRequest, Future[HttpResponse]] = {
    org.bifrost.worker.grpc.WorkerServiceHandler.partial(this)
  }

  override def predict(in: WorkerRequest): Future[PredictReply] = {
    //修改成你要的方法
    def initParam: WorkerRequest => Future[PredictRequest] = dummy

    initParam(in).recoverWith{
      case e: BifrostException => Future.failed(new GrpcServiceException(e.status.withDescription(e.message)))
      case e: Exception => Future.failed(new GrpcServiceException(io.grpc.Status.INVALID_ARGUMENT.withDescription(e.getMessage)))
    }
    .flatMap(predictService.predict)
  }

  def predictMulti(in: WorkerRequests): Future[PredictReplies] = {
    //修改成你要的方法
    def initParam: WorkerRequests => Future[PredictRequests] = dummy

    initParam(in).recoverWith{
      case e: BifrostException => Future.failed(new GrpcServiceException(e.status.withDescription(e.message)))
      case e: Exception => Future.failed(new GrpcServiceException(io.grpc.Status.INVALID_ARGUMENT.withDescription(e.getMessage)))
    }.flatMap(predictService.predictMulti)
  }
}