package org.bifrost.tools

import java.io._

import resource._
import ml.combust.mleap.runtime.MleapSupport._
import ml.combust.bundle.BundleFile
import resource.managed
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, Transformer}
import org.fusesource.scalate.{TemplateEngine, TemplateSource}

import scala.collection.mutable
import scala.collection.mutable.WrappedArray


class Main {

}

object Main {
  val usage = """
    Usage: java -jar bifrost-tools.jar [--output-pb dir] [--output-service dir] modelFile
  """

  def main(args: Array[String]): Unit = {
    /*
    val engine = new TemplateEngine
    val src = TemplateSource.fromURL(getClass.getResource("/test.mustache"))

    val pps = Seq(Map("input" -> Seq(Map("type" -> "string", "name" -> "ggg", "id" -> "1"), Map("type" -> "int", "name" -> "xxx", "id" -> "2")), "mid" -> 2)
                  ,Map("input" -> Seq(Map("type" -> "int", "name" -> "ttt", "id" -> "1"), Map("type" -> "string", "name" -> "nnn", "id" -> "2")), "mid" -> 3)
                  )

    val attributes = Map("pps" -> pps)

    println(engine.layout(src, attributes))
    */

    if (args.length == 0) {
      println(usage)
      System.exit(-1)
    }
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map : OptionMap, list: List[String]) : OptionMap = {
      def isSwitch(s : String) = s(0) == '-'
      list match {
        case Nil => map
        case "--output-pb" :: value :: tail =>
          nextOption(map ++ Map('pbdir -> value), tail)
        case "--output-service" :: value :: tail =>
          nextOption(map ++ Map('servicedir -> value), tail)
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
      System.exit(-1)
    }

    val pbFile = options.getOrElse('pbdir, "./predict.proto").toString
    val serviceFile = options.getOrElse('servicedir, "./PredictServiceImpl.scala").toString
    val model = new File(options('infile).toString)

    val mi = ModelInfo(model.toURI.getPath)

    val pb = mi.renderPbFile()
    val service = mi.renderServiceFile()

    val file = new File(pbFile)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(pb)
    bw.close()

    val file2 = new File(serviceFile)
    val bw2 = new BufferedWriter(new FileWriter(file2))
    bw2.write(service)
    bw2.close()
    System.exit(0)

  }
}
