package cassandra

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.mynt.databases.entity._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._

/**
  * Created by dmitry on 11/2/17.
  */
case class urls0(url:String,s:String)
case class urls(url:String,fingerprintid:String)
class AudienceJobs {
  val sc = new SparkContext
  val spark = SparkSession.builder().appName("spark").enableHiveSupport().getOrCreate()

  //  import java.util.Date
  // set params for all clusters and keyspaces
  spark.setCassandraConf(CassandraConnectorConf.KeepAliveMillisParam.option(10000))

  val profiles = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "profiles3", "keyspace" -> "analytics")).load().cache()
  val events = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "events", "keyspace" -> "analytics")).load().cache()

  import spark.implicits._
  //SOCO DEMO
  profiles.where($"last" === "2017-09-22" && $"myn_id".isNotNull).groupBy($"gender").count().show
  profiles.where($"last" === "2017-09-22" && $"myn_id".isNotNull).show
  profiles.groupBy($"gender", $"last", $"agefrom").count().show
  profiles.groupBy($"sub_purchases", $"last").count().show
  profiles.groupBy($"verticals", $"last").count().show

  val ds_profiles = profiles.as[Profiles]
  //QUERY TOOL
  //simple request
  ds_profiles.where($"last".gt("2017-09-22") && $"myn_id".isNotNull && $"gender" === "u" && $"ageto".lt("12")).show


  val pe = ds_profiles.where($"last".gt("2017-09-22") && $"myn_id".isNotNull && $"gender" === "u" && $"ageto".lt("12")).join(events, events("a7dataid") === profiles("a7data_id")).where($"atype" === "pageview" && $"device_brand" === "Samsung" && $"sourceid".contains("ovo") && $"url".contains("ovo")).select($"sourceid", $"myn_id", $"a7dataid").show

  //select count(*) from events, profiles where events.a7dataid==profiles.a7data_id and age<12 and gender=='u' and last >'2017-09-22' and atype=='pageview';
  //  ds_profiles.filter(l=>if(l.last > new Date("2017-09-22")))

  //verticals, ageto, account_id, gender, a7ids, campaign_exposure, first, a7data_id, myn_id, crm_ids, session_ids,
  // sub_purchases, campaign_interaction, a36e3bf0b86ebe19af3c268705966a294_cookie_ids, last, agefrom, appnexus_ids, purchases
  spark.read.format("csv").option("header", "true").option("mode", "DROPMALFORMED").load("file:///home/bitnami/s3cmd-master/2017-11-27-18-16-20").show
  spark.read.format("json").load("file:///home/bitnami/s3cmd-master/2017-11-27-18-16-20").show
  val d = spark.read.format("json").load("file:///home/bitnami/s3cmd-master/2017-11-27-18-16-20")

  import spark.implicits._

  val data=d.select("url","fingerprintId.*").selectExpr("url", "s as fingerprintid").na.drop.as[urls].map { l => if (l != null && l.url!=null && !l.url.isEmpty) {
    urls(l.url.drop(6).dropRight(3).trim, l.fingerprintid)
  } else {
   urls ("", "")
  }
  }.filter(l => (!l.fingerprintid.isEmpty && !l.url.isEmpty)) //.take(10).foreach(println)


  val data1 = spark.read.format("json").load("file:///home/bitnami/s3cmd-master/2017-11-29-11-56-38")
  val data2 = spark.read.format("json").load("file:///home/bitnami/s3cmd-master/2017-11-29-15-59-16")
  val data3 = spark.read.format("json").load("file:///home/bitnami/s3cmd-master/2017-11-30-13-48-58")
  val df = data//.selectExpr("url", "fingerprintId as fingerprint").toDF()
  //  implicit val cassandraConnector = CassandraConnector(conf)
  import org.apache.spark.sql.SaveMode

  df.na.drop.distinct().write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Overwrite).options(Map("table" -> "urls", "keyspace" -> "analytics", "confirm.truncate" -> "true")).save()

  data.na.drop.distinct().write.format("org.apache.spark.sql.cassandra").mode(SaveMode.Overwrite).options(Map("table" -> "fingerprintlabelshistory", "keyspace" -> "analytics", "confirm.truncate" -> "true")).save()
  spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "urls", "keyspace" -> "analytics")).load().count


}
