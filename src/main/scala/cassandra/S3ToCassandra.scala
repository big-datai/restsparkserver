package cassandra

import com.datastax.spark.connector.cql.CassandraConnectorConf
import com.mynt.databases.entity._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


class S3ToCassandra {


  /**
    * Created by dmitry on 12/21/17.
    */
  //one instance
  //spark/bin/spark-shell --master local --executor-memory 35G --executor-cores 8 --packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.1,com.datastax.spark:spark-cassandra-connector_2.11:2.0.6  --conf spark.cassandra.connection.host=172.31.31.250 --conf spark.cassandra.auth.username=cassandra --conf spark.cassandra.auth.password=

  //cluster instance command
  //spark/bin/spark-shell --master spark://sm:7077 --executor-memory 20G --executor-cores 8 --jars /home/dpavlov/aws-java-sdk-1.7.4.jar,/home/dpavlov/hadoop-aws-2.6.0.jar --packages com.datastax.spark:spark-cassandra-connector_2.11:2.0.6  --conf spark.cassandra.connection.host=10.12.0.20
  class BackUpCassandra extends App{
    val sc = new SparkContext
    val spark = SparkSession.builder().appName("spark").enableHiveSupport().getOrCreate()
    import org.apache.spark.sql.SaveMode

    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", "")
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", "")


    import java.util.Date
    import org.apache.spark.sql.SaveMode

    val profiles = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "profiles5", "keyspace" -> "analytics")).load()
    val events = spark.read.format("org.apache.spark.sql.cassandra").options(Map("table" -> "events4", "keyspace" -> "analytics")).load()

    profiles.write.mode(SaveMode.Append).save("s3n://dimamynt/profiles")
    events.write.mode(SaveMode.Append).save("s3n://dimamynt/events")


    val restore_prof=spark.read.load("s3n://dimamynt/profiles")
    val restore_even=spark.read.load("s3n://dimamynt/events")


    restore_prof.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map("table" -> "profiles3", "keyspace" -> "analytics")).save()
    restore_even.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").options(Map("table" -> "events4", "keyspace" -> "analytics")).save()
  }

}
