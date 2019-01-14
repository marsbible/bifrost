package org.bifrost.worker.common

import akka.http.scaladsl.model.{HttpRequest, HttpResponse}

import scala.concurrent.Future

trait GrpcFunction {
  def partial(): PartialFunction[HttpRequest, Future[HttpResponse]]
}
