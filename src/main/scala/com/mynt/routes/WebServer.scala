import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import spray.json.DefaultJsonProtocol._

import scala.concurrent.Future
import scala.io.StdIn

object WebServer {

//  // domain model
//  final case class Item(name: String, id: String)
//
//  final case class Order(items: List[Item])
//
//  // formats for unmarshalling and marshalling
//  implicit val itemFormat = jsonFormat2(Item)
//  implicit val orderFormat = jsonFormat1(Order)
//
//  // (fake) async database query api
//  def fetchItem(itemId: Long): Future[Option[Item]] = ???
//
//  def saveOrder(order: Item): Future[Done] = ???
//
//  def main(args: Array[String]) {
//
//    // needed to run the route
//    implicit val system = ActorSystem()
//    implicit val materializer = ActorMaterializer()
//    // needed for the future map/flatmap in the end
//    implicit val executionContext = system.dispatcher
//    import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
//
//    case class ResponseBody(status: String, error_msg: String)
//
//    //val sc = new SparkContext
//    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "34.250.184.46")
//      .set("spark.cassandra.auth.username", "cassandra")
//      .set("spark.cassandra.auth.password", "")
//    val spark = SparkSession.builder().config(conf).appName("spark").master("local").getOrCreate()
//
//    //  import java.util.Date
//    // set params for all clusters and keyspaces
//    //    spark.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))
//
//    val profiles = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "profiles", "keyspace" -> "analytics")).load().cache()
//    println(profiles.take(1))
//    val events = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "events", "keyspace" -> "analytics")).load().cache()
//    val route: Route =
//      post {
//        path("sql") {
//          println("hi")
//          entity(as[Item]) { item =>
//            println(item.id)
//            val res = profiles.take(2)
//            complete(HttpEntity(ContentTypes.`text/html(UTF-8)`, s"<h1> finidhed $res</h1>"))
//          }
//        }
//      }
//
//    val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)
//    println(s"Server online at http://localhost:8080/\nPress RETURN to stop...")
//    StdIn.readLine() // let it run until user presses return
//    bindingFuture
//      .flatMap(_.unbind()) // trigger unbinding from the port
//      .onComplete(_ ⇒ system.terminate()) // and shutdown when done
//
//  }
}