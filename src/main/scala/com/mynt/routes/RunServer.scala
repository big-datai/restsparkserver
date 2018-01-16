package com.mynt.routes

import akka.http.scaladsl.Http
import akka.http.scaladsl.marshalling.ToResponseMarshallable
import akka.http.scaladsl.model.{ContentTypes, HttpEntity, HttpResponse, StatusCodes}
import akka.http.scaladsl.server.Directives.{as, complete, entity, get, path, _}
import akka.http.scaladsl.server.Route
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import akka.http.scaladsl.model.StatusCodes._
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.storage.StorageLevel

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.io.StdIn
import scala.util.{Failure, Success}

object RunServer extends Service with RunQuerySevice with LazyLogging {

  // formats for unmarshalling and marshalling
  //implicit val itemFormat = jsonFormat2(Item)
  //implicit val orderFormat = jsonFormat1(Order)

  System.setProperty("hadoop.home.dir", "C:\\workspace\\spark-windows\\winutils")
  val configSpark: Config = ConfigFactory.load().getConfig("application.spark")
  val configServer: Config = ConfigFactory.load().getConfig("application.standalone-http")
  val configCassandra: Config = ConfigFactory.load().getConfig("application.cassandra")

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", configCassandra.getString("host"))
    .set("spark.cassandra.auth.username", configCassandra.getString("username"))
    .set("spark.cassandra.auth.password", configCassandra.getString("password"))
  val spark = SparkSession
    .builder()
    .config(conf)
    .appName("WebServerMyntelligenceCassandra")
    .master(configSpark.getString("master"))
    .getOrCreate()

  val host: String = configServer.getString("host")
  val port: Int = configServer.getInt("port")

  implicit val connectionTimeout: Duration = 2 minutes


  def main(args: Array[String]) {
    val eventsDF = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "events4", "keyspace" -> "analytics"))
      .load().persist(StorageLevel.MEMORY_AND_DISK_2)

    tables.put("events", eventsDF.as('events))
    //eventsDF.createOrReplaceTempView("events")
    //spark.sql("SELECT count(*) FROM events").collect()

    val profilesDF = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "profiles3", "keyspace" -> "analytics"))
      .load().persist(StorageLevel.MEMORY_AND_DISK_2)

    tables.put("profiles", profilesDF.as('profiles))

    //profilesDF.createOrReplaceTempView("profiles")
    //spark.sql("SELECT count(*) FROM profiles").collect()

    val route: Route = {
      logRequestResult("mynt-cassandra") {
        pathPrefix("myntelligence" / "api" / "v1") {
          /*get {
            complete {
              fetchExampleQeury().map[ToResponseMarshallable] {
                case Right(query) => query
                case Left(errorMessage) => BadRequest -> errorMessage
              }
            }
          } ~ */ path("query" / "json") {
            (post & entity(as[String])) { q =>
              complete {
                try {
                  runQueryJSON(q) match {
                    case Success(data) => HttpEntity(ContentTypes.`application/json`, data)
                    case Failure(ex) => HttpResponse(StatusCodes.InternalServerError, entity = s"${ex.getMessage}")
                  }
                } catch {
                  case ex: Throwable =>
                    logger.error(ex.getMessage, ex)
                    HttpResponse(StatusCodes.InternalServerError, entity = s"Error found for query ${ex.getMessage}")
                }
              }
            }
          } ~ path("query" / "sql") {
            (post & entity(as[String])) { q =>
              complete {
                try {
                  OK -> makeQueryBody(q).toString
                } catch {
                  case ex: Throwable =>
                    logger.error(ex.getMessage, ex)
                    StatusCodes.InternalServerError -> s"Error found for query $q [${ex.getMessage}]"
                }
              }
            }
          } ~ path("test") {
            decodeRequest {
              (post & entity(as[String])) { q =>
                complete {
                  try {
                    runQuery(q, spark) match {
                      case Success(data) => HttpEntity(ContentTypes.`application/json`, data)
                      case Failure(ex) => HttpResponse(StatusCodes.InternalServerError, entity = s"${ex.getMessage}")
                    }
                  } catch {
                    case ex: Throwable =>
                      logger.error(ex.getMessage, ex)
                      HttpResponse(StatusCodes.InternalServerError, entity = s"Error found for query: ${ex.getMessage}")
                  }
                }
              }
            }
          }
        }
      }
    }

    val bindingFuture =
      Http().bindAndHandle(route, host, port)
    println(s"Server online at http://localhost:9090/\nPress RETURN to stop...")
    info //StdIn.readLine() // let it run until user presses return
    bindingFuture
      //.flatMap(_.unbind()) // trigger unbinding from the port
      .onFailure {
      case err ⇒ system.terminate()
    } // and shutdown when done
  }

  private def info(): Unit = {
    logger.info(s"  - Configuration: Start server at $host $port on ActorSystem(${system.name})")
  }

}
