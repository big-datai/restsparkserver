package cassandra

import cassandra.ES2Cassandra.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.LinkedHashMap

/**
  * Created by dmitry on 1/14/18.
  */

//spark/bin/spark-shell --master "local[14]" --driver-memory 45G --executor-cores 8 --jars /home/bitnami/jars/aws-java-sdk-1.7.4.jar,/home/bitnami/jars/hadoop-aws-2.6.0.jar --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.6 --conf spark.cassandra.connection.host=52.212.77.202 --conf spark.cassandra.auth.username=cassandra --conf spark.cassandra.auth.password=""
class Dynamo2Cassandra {
  val sc = new SparkContext
  val spark = SparkSession.builder().appName("spark").enableHiveSupport().getOrCreate()
  import org.apache.spark.sql.SaveMode

  sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "")
  sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "")


  import java.util.Date
  import org.apache.spark.sql.SaveMode

//  val profiles = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "profiles3", "keyspace" -> "analytics")).load()
//  val events = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "events4", "keyspace" -> "analytics")).load()

//  profiles.write.mode(SaveMode.Append).save("s3n://dimamynt/profiles")
//  events.write.mode(SaveMode.Append).save("s3n://dimamynt/events")
//
  import spark.implicits._
  val ddt=spark.read.json("s3n://dimamynt/dynamodb/DashboardDataTable/*/").withColumnRenamed("_corrupt_record", "corrupt_record")
  ddt.na.drop(Seq("page_url")).write.mode(SaveMode.Ignore).format("org.apache.spark.sql.cassandra").options(Map("table" -> "dashboarddatatable", "keyspace" -> "analytics")).save()

  val usereventshistory=spark.read.json("s3n://dimamynt/dynamodb/UserEventsHistory/*/").withColumnRenamed("_corrupt_record", "corrupt_record").withColumnRenamed("fingerprintId", "fingerprintid").withColumnRenamed("timestampMilliseconds", "timestampmilliseconds")
  usereventshistory.na.drop(Seq("url","fingerprintid")).write.mode(SaveMode.Ignore).format("org.apache.spark.sql.cassandra").options(Map("table" -> "usereventshistory", "keyspace" -> "analytics")).save()

  val userevents=spark.read.json("s3n://dimamynt/dynamodb/UserEvents/*/").withColumnRenamed("_corrupt_record", "corrupt_record").withColumnRenamed("fingerprintId", "fingerprintid").withColumnRenamed("timestampMilliseconds", "timestampmilliseconds")
  userevents.na.drop(Seq("url")).write.mode(SaveMode.Ignore).format("org.apache.spark.sql.cassandra").options(Map("table" -> "userevents", "keyspace" -> "analytics")).save()

  import com.datastax.spark.connector.cql.{ColumnDef, RegularColumn, TableDef, ClusteringColumn, PartitionKeyColumn}
  import com.datastax.spark.connector.types._
  import com.datastax.spark.connector._
  import org.apache.spark.sql.Row
  import org.apache.spark.sql.functions._
  import scala.collection.mutable.WrappedArray
  val textclassification=spark.read.json("s3n://dimamynt/dynamodb/TextClassificationCache/*/").withColumnRenamed("_corrupt_record", "corrupt_record").withColumnRenamed("fingerprintId", "fingerprintid").withColumnRenamed("timestampMilliseconds", "timestampmilliseconds").withColumnRenamed("expirationMilliseconds", "expirationmilliseconds").withColumnRenamed("overallSentiment", "overallsentiment").withColumnRenamed("subVerticals", "subverticals").withColumnRenamed("subPurchases", "subpurchases").withColumnRenamed("urlId", "urlid")
  val text=textclassification.na.drop(Seq("timestampmilliseconds"))




//  val ftext=text.map{
//    l=>
//      val attributes = l.getAs[WrappedArray[String]]("keywords")
//      //val attMapAge = l.getOrElse(LinkedHashMap()).asInstanceOf[LinkedHashMap[String, LinkedHashMap[String, Long]]]
//      attributes
//  }

  //.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map("table" -> "textclassificationcache", "keyspace" -> "analytics")).save()



  // textclassification.na.drop(Seq("timestampmilliseconds")).createCassandraTable("analytics","textclassificationcache")

  //format("org.apache.spark.sql.cassandra").options(Map("table" -> "textclassificationcache", "keyspace" -> "analytics","clusteringKeys" -> "timestampmilliseconds")).save()


//  table1.createCassandraTable("test", "otherwords", partitionKeyColumns = Some(Seq("word")), clusteringKeyColumns = Some(Seq("count")))

  import com.datastax.spark.connector.cql.{ColumnDef, RegularColumn, TableDef}
  import com.datastax.spark.connector.types.IntType
//  textclassification.na.drop(Seq("timestampmilliseconds")).rdd.saveAsCassandraTable("analytics","textclassificationcache")

  //test read
  spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "textclassificationcache", "keyspace" -> "analytics")).load()




  //val profiles = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "dashboarddatatable", "keyspace" -> "analytics")).load()
}
