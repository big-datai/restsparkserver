package com.mynt.client

import java.net.URL

import scala.util.Random


//import java.nio.charset.StandardCharsets._
//import org.asynchttpclient.Dsl._


object AsyncClient {

  def main(args: Array[String]): Unit = {
//    import java.util.concurrent.CompletableFuture

val r = Random

    val l1 = List(1,2,3)
    val l2 = List(3,4,5)
    val s3 = Set(1,2)
    val s4 = Set(1,2)
    val s5 = Set(1,2)

    (for {
      a1 <- l1
      a2 <- l2
      a3 <- s3
      a4 <- s4
      a5 <- s5
    } yield s"elem: $a1, $a2, $a3, $a4, $a5")(scala.collection.breakOut)

//    (1 to 10).par.foreach {
//
//      i => println(s"${r.nextInt(7)}")
//    }
//    val aClient = asyncHttpClient
    /*val promise = asyncHttpClient.preparePost("http://www.example.com/").execute.toCompletableFuture.exceptionally((t) => {
      def foo(t) = {
        /* Something wrong happened... */
      }

      foo(t)
    }).thenApply((resp) => {
      def foo(resp) = /*  Do something with the Response */resp

      foo(resp)
    })*/
  }
}
