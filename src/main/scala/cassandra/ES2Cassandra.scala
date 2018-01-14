package cassandra

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, LinkedHashMap, Map}
import scala.util.Success
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import scala.collection.mutable.Buffer
import scala.util.Try

/**
  * Created by dmitry on 10/26/17.
  */
object ES2Cassandra extends App {
  // ./start-thriftserver.sh --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.5-s2.11 --conf spark.cassandra.connection.host=127.0.0.1 --conf spark.cassandra.auth.username=cassandra --conf spark.cassandra.auth.password=""
  //./start-thriftserver.sh --packages org.elasticsearch:elasticsearch-spark-20_2.11:6.0.0-alpha-1,datastax:spark-cassandra-connector:2.0.5-s_2.11 --conf spark.cassandra.connection.host=127.0.0.1 --conf spark.cassandra.auth.username=cassandra --conf spark.cassandra.auth.password=""
  //  ./spark/bin/spark-shell --master "local[*]" --packages org.elasticsearch:elasticsearch-spark-20_2.11:6.0.0-alpha-1,datastax:spark-cassandra-connector:2.0.5-s_2.11 --conf spark.es.nodes="34.252.202.146"  --conf spark.es.port="9200" --conf spark.es.index.auto.create=true  --conf spark.es.nodes.wan.only=true --conf spark.cassandra.connection.host=127.0.0.1 --conf spark.cassandra.auth.username=cassandra --conf spark.cassandra.auth.password=""

  //  ./start-thriftserver.sh --master "local[2]" --packages datastax:spark-cassandra-connector:2.0.5-s_2.11 --conf spark.cassandra.connection.host=127.0.0.1 --conf spark.cassandra.auth.username=cassandra --conf spark.cassandra.auth.password=""
  //  System.setProperty("hadoop.home.dir", "C:\\workspace\\spark-windows\\winutils")
  //
  val conf = new SparkConf(true)
  //    .set("spark.cassandra.connection.host", "52.212.77.202")
  //    .set("spark.cassandra.auth.username", "cassandra")
  //    .set("spark.cassandra.auth.password", "")
  val spark = SparkSession
    .builder()
    .config(conf)
    .appName("WebServerMyntelligenceCassandra")
    //.master("spark://ec2-52-212-77-202.eu-west-1.compute.amazonaws.com:7077")
    .getOrCreate()
  val profiles = spark.sparkContext.objectFile[Tuple2[String, scala.collection.mutable.Map[String, AnyRef]]]("file:///home/bitnami/backup1/profiles").map {
    l =>
      val profileId = l._1
      val profileRow = l._2
      val attributes = profileRow.get("attributes")
      val attributesMapValuesString = attributes.getOrElse(LinkedHashMap()).asInstanceOf[LinkedHashMap[String, String]]
      val attMapValuesLong = attributes.getOrElse(LinkedHashMap()).asInstanceOf[LinkedHashMap[String, Long]]
      val attMapAge = attributes.getOrElse(LinkedHashMap()).asInstanceOf[LinkedHashMap[String, LinkedHashMap[String, Long]]]
      val gender = Try(attributesMapValuesString.getOrElse("gender", "u")).getOrElse("u")
      val accountId = Try(attMapValuesLong.getOrElse("account_id", 0L)).getOrElse(0L)
      val lineItemIds = Try(attMapValuesLong.getOrElse("line_item_id", 0L)).getOrElse(0L)
      val creativeId = Try(attMapValuesLong.getOrElse("creative_id", 0L)).getOrElse(0L)
      val ageFrom = Try(attMapAge.get("age").map(_ ("from")).get).getOrElse(0L)
      val ageTo = Try(attMapAge.get("age").map(_ ("to")).get).getOrElse(0L)

      //val lists = attributes.getOrElse(LinkedHashMap.empty[String, Buffer[Long]]).asInstanceOf[LinkedHashMap[String, Buffer[Long]]]
      val listsString = attributes.getOrElse(LinkedHashMap.empty[String, Buffer[String]]).asInstanceOf[LinkedHashMap[String, Buffer[String]]]
      val datasourceIds = Try(listsString.getOrElse("crm_ids", Buffer.empty[String])).getOrElse(Buffer.empty[String])
      val campaignExposures = Try(listsString("campaign_exposure")).getOrElse(Buffer.empty[String])
      val campaignInteractions = Try(listsString("campaign_interaction")).getOrElse(Buffer.empty[String])

      val verticals = Try(listsString("verticals")).getOrElse(Buffer.empty[String])
      val subVerticals = Array[Long](0L) //TODO
      val purchases = Try(listsString("purchases")).getOrElse(Buffer.empty[String])
      val subPurchases = Try(listsString("sub_purchases")).getOrElse(Buffer.empty[String])

      val ids = profileRow.get("ids")
      val idsMap = ids.getOrElse(LinkedHashMap.empty[String, Buffer[String]]).asInstanceOf[LinkedHashMap[String, Buffer[String]]]
      val mynIds = Try(idsMap.getOrElse("myn_id", Buffer("none"))).getOrElse(Buffer("none")) //Try(idsMap.getOrElse("myn_id", Buffer("none"))).getOrElse(Buffer("none"): _*))
      val appnexusIds = Try(idsMap.getOrElse("appnexus_ids", Buffer("none"))).getOrElse(Buffer("none"))
      val profileIds = Try(idsMap.getOrElse("7data_id", Buffer("none"))).getOrElse(Buffer("none"))
      val activity = profileRow.get("activity")

      val activityVals = activity.map { m =>
        (m.asInstanceOf[LinkedHashMap[String, java.util.Date]].get("first"), m.asInstanceOf[LinkedHashMap[String, java.util.Date]].get("last"))
      }.getOrElse((None, None))

      val activityFirst = new java.sql.Date(activityVals._1.map(_.getTime).getOrElse(0L))
      val activityLast = new java.sql.Date(activityVals._2.map(_.getTime).getOrElse(0L))
      val enrichedAt = new java.sql.Date(Try(profileRow("socioDataEnrichedAt").asInstanceOf[java.util.Date]).map(_.getTime).getOrElse(0L))

      (
        profileId
        , activityFirst
        , activityLast
        , erase(mynIds)
        , gender
        , ageFrom
        , ageTo
        , erase(appnexusIds)
        , enrichedAt
        , accountId
        , eraseLong(verticals)
        , subVerticals
        , eraseLong(purchases)
        , eraseLong(subPurchases)
        , erase(datasourceIds)
        , eraseLong(campaignInteractions)
        , eraseLong(campaignExposures)
        , lineItemIds
        , creativeId
        , erase(profileIds)
      )
  }

  import spark.implicits._

  val pc = profiles.toDF() //.persist(StorageLevel.MEMORY_ONLY_2)
  val uuid = udf(() => java.util.UUID.randomUUID().toString)

  import spark.implicits._
  val profilesDF = pc.withColumn("verticals", explode($"_11"))
    .withColumn("sub_verticals", explode($"_12"))
    .withColumn("purchases", explode($"_13"))
    .withColumn("sub_purchases", explode($"_14"))
    .withColumn("campaign_interactions", explode($"_16"))
    .withColumn("campaign_exposures", explode($"_17"))
    .withColumn("id", uuid())

  val profilesDB = profilesDF
    .selectExpr("id"
      , "_1 as profile_id"
      , "_2 as activity_first"
      , "_3 as activity_last"
      , "_4 as myn_ids"
      , "_5 as gender"
      , "_6 as age_from"
      , "_7 as age_to"
      , "_8 as appnexus_ids"
      , "_9 as enriched_at"
      , "_10 as account_id"
      , "verticals"
      , "sub_verticals"
      , "purchases"
      , "sub_purchases"
      , "_15 as datasource_ids"
      , "campaign_interactions"
      , "campaign_exposures"
      , "_18 as lineitem_ids"
      , "_19 as creative_ids"
      , "_20 as profile_ids"
    )
  profilesDB.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map("table" -> "profiles3", "keyspace" -> "analytics")).save()
  //  pdf.write.format("org.apache.spark.sql.cassandra").options(Map("table" -> "users", "keyspace" -> "analytics")).save()
  //  sc.objectFilee()
  spark.sparkContext.objectFile[Tuple2[String, scala.collection.mutable.Map[String, AnyRef]]]("file:///home/bitnami/backup1/events").count()
  val events = spark.sparkContext.objectFile[Tuple2[String, scala.collection.mutable.Map[String, AnyRef]]]("file:///home/bitnami/backup1/events").map {
    event =>
      //Set(source, sourceId, domain, path, type, payload, device, os, location, createdAt, 7dataId)
      val id = event._1
      val event_row = event._2

      val events_flat = Try(event_row.asInstanceOf[LinkedHashMap[String, String]])
      val source = Try(events_flat.get("source")).getOrElse("none")
      val sourceId = Try(events_flat.get("sourceId")).getOrElse("none")
      val domain = Try(events_flat.get("domain")).getOrElse("none")
      val path = Try(events_flat.get("path")).getOrElse("none")
      val event_type = Try(events_flat.get("type")).getOrElse("none")
      val events_flat_date = Try(event_row.asInstanceOf[LinkedHashMap[String, java.util.Date]])
      val createdAt = Try(new java.sql.Date(events_flat_date.get("createdAt").getTime)).getOrElse(new java.sql.Date(0))
      val profile_id = Try(events_flat.get("7dataId")).getOrElse("none")

      val payload = Try(event_row("payload"))
      val payload_map_string = Try(payload.get.asInstanceOf[LinkedHashMap[String, String]])
      val payload_map_long = Try(payload.get.asInstanceOf[LinkedHashMap[String, Long]])
      val payload_map_age = Try(payload.get.asInstanceOf[LinkedHashMap[String, LinkedHashMap[String, Long]]])
      val payload_map_list = Try(payload.get.asInstanceOf[LinkedHashMap[String, Buffer[String]]])

      val referrer = Try(payload_map_string.get("referrer")).getOrElse("none")
      val cookie_key = Try(payload_map_string.get("key")).getOrElse("none")
      val cookie_value = Try(payload_map_string.get("value")).getOrElse("none")
      val document_referrer = Try(payload_map_string.get("document_referrer")).getOrElse("none")
      val url = Try(payload_map_string.get("url")).getOrElse("none")
      val gender = Try(payload_map_string.get("gender")).getOrElse("none")
      val account_id = Try(payload_map_long.get("account_id")).getOrElse(0L)

      val authenticated = Try(payload_map_long.get("authenticated")).getOrElse(0L)
      val loyalty = Try(payload_map_long.get("loyalty")).getOrElse(0L)
      val campaign_id = Try(payload_map_long.get("campaign_id")).getOrElse(0L)
      val lineitem_id = Try(payload_map_long.get("lineitem_id")).getOrElse(0L)
      val creative_id = Try(payload_map_long.get("creative_id")).getOrElse(0L)
      val age_from = Try(payload_map_age.get("age")("from")).getOrElse(0L)
      val age_to = Try(payload_map_age.get("age")("to")).getOrElse(0L)
      val verticals = Try(payload_map_list.get("verticals")).getOrElse(Buffer.empty[String])
      val sub_verticals = Array.empty[Long]
      val purchases = Try(payload_map_list.get("purchases")).getOrElse(Buffer.empty[String])
      val sub_purchases = Try(payload_map_list.get("sub_purchases")).getOrElse(Buffer.empty[String])

      val device = Try(event_row("device"))
      val device_map = Try(device.get.asInstanceOf[LinkedHashMap[String, String]])
      val device_type = Try(device_map.get("type")).getOrElse("none")
      val device_brand = Try(device_map.get("brand")).getOrElse("none")
      val device_model = Try(device_map.get("model")).getOrElse("none")
      val device_browser = Try(device_map.get("browser")).getOrElse("none")

      val os = Try(event_row("os"))
      val os_map_string = Try(os.get.asInstanceOf[LinkedHashMap[String, String]])
      val os_name = Try(os_map_string.get("name")).getOrElse("none")
      val os_version = Try(os_map_string.get("version")).getOrElse("none")

      val location = Try(event_row("location"))
      val location_string = Try(location.get.asInstanceOf[LinkedHashMap[String, String]])
      val location_lat = Try(location_string.get("lat")).getOrElse("none")
      val location_lng = Try(location_string.get("lng")).getOrElse("none")
      val location_country = Try(location_string.get("country")).getOrElse("none")
      val location_city = Try(location_string.get("city")).getOrElse("none")
      val location_region = Try(location_string.get("regions")).getOrElse("none")

      Event(id
        , source
        , sourceId
        , domain
        , path
        , event_type
        , createdAt
        , profile_id
        , referrer
        , document_referrer
        , url
        , gender
        , account_id
        , authenticated == 1L
        , loyalty == 1L
        , age_from
        , age_to
        , os_name
        , os_version
        , location_lat
        , location_lng
        , location_country
        , location_city
        , location_region
        , cookie_key
        , cookie_value
        , device_type
        , device_brand
        , device_model
        , device_browser
        , eraseLong(verticals)
        , sub_verticals
        , eraseLong(purchases)
        , eraseLong(sub_purchases)
        , campaign_id
        , lineitem_id
        , creative_id
        , new java.sql.Date(System.currentTimeMillis()) // enriched_at date
      )
  }

  events.toDF.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map("table" -> "events3", "keyspace" -> "analytics")).save()

  val profiles3DB = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "profiles3", "keyspace" -> "analytics")).load().cache()
  val events3DB = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "events3", "keyspace" -> "analytics")).load().cache()

  val p = profiles3DB.withColumn("p_profile", explode($"profile_ids")).selectExpr("profile_id as p_profile_id", "p_profile as profile_id") //.createOrReplaceTempView("profiles")

  val events4 = events3DB.join(p, "profile_id").drop("profile_id").withColumnRenamed("p_profile_id", "profile_id")



  def eraseLong(seq: Buffer[String]): Array[Long] = {
    val array = erase(seq, "0")
    val result = array.map { v =>
      Try(v.toLong) match {
        case Success(u) => u
        case _ => 0L
      }
    }

    if (result.isEmpty) Array[Long](0L) else result
  }

  def erase(buf: Buffer[String], defaultVal: String = "none") = {
    try {
      buf match {
        case ar: ArrayBuffer[String] => ar.toArray
        case b: Buffer[String] => if (b == null || b.isEmpty) Array(defaultVal) else b.toArray
        case _ => Array(defaultVal)
      }
    } catch {
      case _: Throwable => Array(defaultVal)
    }
  }
}

case class Event(
                  id: String
                 , source: String
                 , sourceid: String
                 , domain: String
                 , path: String
                 , event_type: String
                 , created_at: java.sql.Date
                 , profile_id: String
                 , referrer: String
                 , document_referrer: String
                 , url: String
                 , gender: String
                 , account_id: Long
                 , authenticated: Boolean
                 , loyalty: Boolean
                 , age_from: Long
                 , age_to: Long
                 , os_name: String
                 , os_version: String
                 , location_lat: String
                 , location_lng: String
                 , location_country: String
                 , location_city: String
                 , location_region: String
                 , cookie_key: String
                 , cookie_value: String
                 , device_type: String
                 , device_brand: String
                 , device_model: String
                 , device_browser: String
                 , verticals: Array[Long]
                , sub_verticals: Array[Long]
                , purchases: Array[Long]
                , sub_purchases: Array[Long]
                , campaign_id: Long
                , lineitem_id: Long
                , creative_id: Long
                , enriched_at: java.sql.Date
                )

