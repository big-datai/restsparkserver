package cassandra

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/**
  * Created by dmitry on 12/21/17.
  */
//one instance
//spark/bin/spark-shell --master local --executor-memory 35G --executor-cores 8 --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.1,com.datastax.spark:spark-cassandra-connector_2.11:2.0.6  --conf spark.cassandra.connection.host=172.31.31.250 --conf spark.cassandra.auth.username=cassandra --conf spark.cassandra.auth.password=

//cluster instance command
//spark/bin/spark-shell --master spark://sm:7077 --executor-memory 20G --executor-cores 8 --jars /home/dpavlov/aws-java-sdk-1.7.4.jar,/home/dpavlov/hadoop-aws-2.6.0.jar --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.6  --conf spark.cassandra.connection.host=10.12.0.20
class BackUpCassandra extends App {
  val sc = new SparkContext
  val spark = SparkSession.builder().appName("spark").enableHiveSupport().getOrCreate()

  sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "")
  sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "")
  import org.apache.spark.sql.SaveMode
  val profiles = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "profiles3", "keyspace" -> "analytics")).load()
  val events = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "events4", "keyspace" -> "analytics")).load()
  val urls = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "urls", "keyspace" -> "analytics")).load()
  profiles.write.mode(SaveMode.Overwrite).save("s3n://dimamynt/profiles3")
  events.write.mode(SaveMode.Overwrite).save("s3n://dimamynt/events4")
 urls.write.mode(SaveMode.Overwrite).save("s3n://dimamynt/urls")
//RESTORE

  sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "")
  sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "")
  import org.apache.spark.sql.SaveMode
  val restore_prof = spark.read.load("s3n://dimamynt/profiles3")
  val restore_even = spark.read.load("s3n://dimamynt/events4")
  val resotre_urls = spark.read.load("s3n://dimamynt/urls")
//write_request_timeout_in_ms: 20000
  restore_prof.repartition(150).write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map("table" -> "profiles3", "keyspace" -> "analytics")).save()
  restore_even.repartition(150).write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map("table" -> "events4", "keyspace" -> "analytics")).save()
  resotre_urls.repartition(150).write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map("table" -> "urls", "keyspace" -> "analytics")).save()

val pe=profiles.join(events,profiles("profile_id")===events("profile_id"),"left_outer")


}
