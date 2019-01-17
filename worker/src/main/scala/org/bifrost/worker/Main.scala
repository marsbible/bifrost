package org.bifrost.worker

import java.io.{File, IOException}

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.grpc.scaladsl.ServiceHandler
import akka.http.scaladsl.UseHttp2.Always
import akka.http.scaladsl.model.{HttpRequest, HttpResponse}
import akka.http.scaladsl.{Http, HttpConnectionContext}
import akka.stream.{ActorMaterializer, Materializer}
import com.google.common.reflect.ClassPath
import com.typesafe.config.{Config, ConfigFactory, ConfigObject, ConfigValue}
import org.bifrost.worker.service.{PredictServiceImpl, WorkerServiceImpl}
import org.apache.solr.client.solrj.impl.{CloudSolrClient, LBHttpSolrClient}
import redis.{RedisClient, RedisDispatcher}

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, Future}
import com.sksamuel.elastic4s.http.{ElasticClient, ElasticProperties, Executor, Response}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.solr.client.solrj.SolrClient
import org.bifrost.worker.common.GrpcFunction
import slick.jdbc.MySQLProfile.api._
import org.mongodb.scala._
import org.mongodb.scala.connection.NettyStreamFactoryFactory

import scala.concurrent.duration.Duration


object Main {
  def main(args: Array[String]): Unit = {
    // Important: enable HTTP/2 in ActorSystem's config
    // We do it here programmatically, but you can also set it in the reference.conf
    val conf = ConfigFactory.load()
    val system = ActorSystem("worker", conf)
    new Main(system, conf).run(args)
    // ActorSystem threads will keep the app alive until `system.terminate()` is called
  }
}

class Main(system: ActorSystem, conf: Config) {
  private val usage = """
    Usage: java -jar bifrost-worker.jar [--ip ip] [--port port] modelFile
  """
  // Akka boot up code
  implicit val sys: ActorSystem = system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = sys.dispatcher
  implicit val logger: LoggingAdapter = sys.log
  implicit val cnf: Config = conf

  private val CONF_PREFIX = "datasources"
  private var clients: Map[String, Object] = Map()
  private var dispatchers : Map[String, ExecutionContext] = Map()
  private val defaultDispatcher = sys.dispatchers.lookup("akka.stream.default-blocking-io-dispatcher")

  def getExecutionContext(name: String):  ExecutionContext = {
    dispatchers.getOrElse(name, defaultDispatcher)
  }

  def getRedisClient(name: String): RedisClient = {
    clients(name).asInstanceOf[RedisClient]
  }

  def getSolrClient(name: String): SolrClient = {
    clients(name).asInstanceOf[SolrClient]
  }

  def getHbaseClient(name: String): org.apache.hadoop.hbase.client.Connection = {
    clients(name).asInstanceOf[org.apache.hadoop.hbase.client.Connection]
  }

  def getEsClient(name: String): ElasticClient = {
    clients(name).asInstanceOf[ElasticClient]
  }

  def getSqlClient(name: String): Database = {
    clients(name).asInstanceOf[Database]
  }

  def getMongoClient(name: String): MongoDatabase = {
    clients(name).asInstanceOf[MongoDatabase]
  }

  //调用时需要在future中使用
  private def solrClient(cf: Config): Option[SolrClient] = {
    // https://lucene.apache.org/solr/guide/7_6/using-solrj.html#using-solrj
    if(!cf.hasPath("enabled") || !cf.getBoolean("enabled")) {
      return None
    }

    if(cf.hasPath("zkHosts")) {
      import org.apache.solr.client.solrj.impl.NoOpResponseParser
      val responseParser = new NoOpResponseParser
      responseParser.setWriterType("json")

      val root = if(cf.hasPath("zkChroot")) cf.getString("zkChroot") else "/solr"
      val csc = new CloudSolrClient.Builder(cf.getStringList("zkHosts"), java.util.Optional.of(root)).build()
      Some(csc)
    }
    else {
      val nodes = cf.getStringList("nodes").asScala
      val lbh = new LBHttpSolrClient.Builder().withBaseSolrUrls(nodes: _*).build()
      Some(lbh)
    }
  }

  //调用时需要在future中使用
  private def hbaseClient(cf: Config): Option[org.apache.hadoop.hbase.client.Connection] = {
    // https://hbase.apache.org/1.2/devapidocs/org/apache/hadoop/hbase/client/package-summary.html
    if(!cf.hasPath("enabled") || !cf.getBoolean("enabled")) {
      return None
    }
    val hc = HBaseConfiguration.create()
    val cl = cf.getObject( "client")
    for(entry <- cl.entrySet().asScala) {
      hc.set(entry.getKey, entry.getValue.unwrapped().toString)
    }

    val connection = ConnectionFactory.createConnection(hc)

    Some(connection)
  }

  // 内部支持线程池，不用在future中调用
  private def redisClient(cf: Config): Option[RedisClient] = {
    // https://github.com/Ma27/rediscala
    if(!cf.hasPath("enabled") || !cf.getBoolean("enabled")) {
      return None
    }
    val redis = RedisClient(cf.getString("host"),
                            cf.getInt("port"),
                            if(cf.hasPath("password")) Some(cf.getString("password")) else None,
                            if(cf.hasPath("db")) Some(cf.getInt("db")) else None
                            )

    Some(redis)
  }

  //内部支持线程池，不用在future中调用
  private def esClient(cf: Config): Option[ElasticClient] = {
    // https://github.com/sksamuel/elastic4s
    if(!cf.hasPath("enabled") || !cf.getBoolean("enabled")) {
      return None
    }
    // we must import the dsl
    import com.sksamuel.elastic4s.http.ElasticDsl._

    //es client使用内部的线程池和异步io处理请求，不会阻塞我们的线程
    val client = ElasticClient(ElasticProperties(cf.getString("uri")))

    Some(client)
  }

  private def sqlClient(cf: Config): Option[Database] = {
    //http://slick.lightbend.com/doc/3.2.3/sql.html
    if(!cf.hasPath("enabled") || !cf.getBoolean("enabled")) {
      return None
    }

    val db = Database.forConfig("", cf)

    Some(db)
  }

  private def mongoClient(cf: Config): Option[MongoDatabase] = {
    //https://mongodb.github.io/mongo-scala-driver/2.5/getting-started/
    if(!cf.hasPath("enabled") || !cf.getBoolean("enabled")) {
      return None
    }
    MongoClientSettings.builder().streamFactoryFactory(NettyStreamFactoryFactory()).build()
    val mongoClient: MongoClient = MongoClient(cf.getString("uri"))

    val dbNames = Await.result(mongoClient.listDatabaseNames().toFuture(), Duration.create(5000, "milli"))
    val database: MongoDatabase = mongoClient.getDatabase(dbNames.filterNot(_.equals("local")).head)

    Some(database)
  }

  //创建的所有客户端都必须是线程安全的
  private def createClients(): Map[String, Object] = {
    val cl = conf.getObject(CONF_PREFIX)

    cl.asScala.map {
      case (k, v) =>
        val t = k.split('-').apply(0)
        val cf = v.asInstanceOf[ConfigObject].toConfig

        if(cf.hasPath("dispatcher")) {
          // else "akka.stream.default-blocking-io-dispatcher"
          val dispatcher = sys.dispatchers.lookup(cf.getString("dispatcher"))
          dispatchers += (k -> dispatcher)
        }

        t match {
          case "redis" => (k,redisClient(cf))
          case "hbase" => (k,hbaseClient(cf))
          case "solr" => (k,solrClient(cf))
          case "es" => (k,esClient(cf))
          case "mysql" => (k,sqlClient(cf))
          case "mongo" => (k,mongoClient(cf))
          case _ =>
            logger.warning("Invalid client name: {}", k)
            (k, None)
        }
    }.collect { case (k, Some(v)) => (k, v) }.toMap
  }

  def getClassOfPackage(packagenom: String): Seq[ClassPath.ClassInfo]  = {
    val loader = Thread.currentThread.getContextClassLoader
    try {
      val classpath = ClassPath.from(loader) // scans the class path used by classloader
      import scala.collection.JavaConversions._

      classpath.getTopLevelClasses(packagenom).filterNot(_.getSimpleName.endsWith("_")).toSeq
    } catch {
      case e: IOException =>
        e.printStackTrace()
        Seq()
    }
  }

  def run(args: Array[String]): Future[Http.ServerBinding] = {
    clients = createClients()

    val classes = getClassOfPackage("org.bifrost.worker.service")

    if (args.length == 0) {
      println(usage)
      System.exit(0)
    }
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]): OptionMap = {
      def isSwitch(s : String) = s(0) == '-'

      list match {
        case Nil => map
        case "--ip" :: value :: tail =>
          nextOption(map ++ Map('ip -> value), tail)
        case "--port" :: value :: tail =>
          nextOption(map ++ Map('port -> value.toInt), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          nextOption(map ++ Map('infile -> string), list.tail)
        case string :: Nil =>  nextOption(map ++ Map('infile -> string), list.tail)
        case option :: tail =>
          println("Unknown option "+option)
          map
      }
    }

    val options = nextOption(Map(),arglist)
    if(!options.contains('infile)) {
      println(usage)
      System.exit(0)
    }

    val predict = new PredictServiceImpl(this, new File(options('infile).toString).toURI.getPath)

    //初始化所有的service
    val allServices = classes.map(ci => {
      if(ci.getSimpleName.equals("PredictServiceImpl")) {
        org.bifrost.worker.grpc.PredictServiceHandler.partial(predict)
      }
      else {
        val i = Class.forName(ci.getName).getConstructor(this.getClass, predict.getClass).newInstance(this, predict)
        i.asInstanceOf[GrpcFunction].partial()
      }
    })

    // Bind service handler server to localhost:8090
    val serviceHandlers =
      ServiceHandler.concatOrNotFound(allServices: _*)

    val bound = Http().bindAndHandleAsync(
      serviceHandlers,
      interface = options.getOrElse('ip, conf.getString("server.bind")).asInstanceOf[String],
      port = options.getOrElse('port, conf.getInt("server.port")).asInstanceOf[Int],
      connectionContext = HttpConnectionContext(http2 = Always))

    // report successful binding
    bound.foreach { binding =>
      logger.info(s"gRPC server bound to: ${binding.localAddress}")
    }
    bound
  }
}
