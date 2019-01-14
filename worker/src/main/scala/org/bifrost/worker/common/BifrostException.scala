package org.bifrost.worker.common

import akka.grpc.GrpcServiceException

class BifrostException(val status: io.grpc.Status, val message: String) extends RuntimeException(message) {
  override def fillInStackTrace: Throwable = { // do nothing
    this
  }
}