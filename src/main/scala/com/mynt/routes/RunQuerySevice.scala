package com.mynt.routes

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.datastax.spark.connector.cql.CassandraConnector
import com.mynt.databases.entity.Profiles
import com.mynt.routes.RunServer.conf
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.cassandra._
import spray.json.JsArray

import scala.collection.convert.Wrappers.MutableSetWrapper
import scala.concurrent.Future
import scala.util.Try
import scala.util.parsing.json.{JSONArray, JSONObject}

trait RunQuerySevice extends LazyLogging {

  // Add spark connector specific methods to Dataset
  import com.datastax.spark.connector._
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  // needed for the future map/flatmap in the end
  implicit val executionContext = system.dispatcher


  def runQuery(query: String, spark: SparkSession): Try[Array[Byte]] = {
    Try(spark.sql(query).toJSON.collect().mkString("[", "," , "]").getBytes)
  }

  def convertRowToJSON(row: Row): String = {
    val m = row.getValuesMap(row.schema.fieldNames)
    JSONObject(m).toString()
  }

  def runQueryJSON(query: Query, spark: SparkSession) = {
    runQuery(makeQueryBody(query), spark)
  }

  def makeQueryBody(query: Query): String = {
    val select = s"SELECT ${query.select.mkString(", ")}"
    val join = getJoin(query)
    val from = s"FROM ${query.table}"
    val where = query.filter.map(w => getWhere(w, query.table)).getOrElse("")
    val groupBy = query.groupBy.map(g => s"GROUP BY ${g.mkString(", ")}").getOrElse("")
    val orderBy = query.orderBy.map(o => s"ORDER BY ${o.mkString(", ")}").getOrElse("")
    val limit = query.limit.map(l => s"LIMIT $l").getOrElse("")

    logger.debug(s"Query: $select $from $where $groupBy $orderBy $limit")

    s"$select $from $join $where $groupBy $orderBy $limit"
  }

  def getJoin(query: Query) = {
    query.join.map {
      j => s"${j.joinType.getOrElse("INNER")} JOIN ${j.table} ON ${j.table}.${j.columnOn} = ${query.table}.${j.columnTo.getOrElse(j.columnOn)}"
    }.getOrElse("")
  }

  def getWhere(filter: Filter, table: String) = {
    val and = filter.and.map(l => l.map(getExpression(_, table)).mkString("(", " AND ", ")")).orNull
    val or = filter.or.map(l => l.map(getExpression(_, table)).mkString("(", " OR ", ")")).orNull
    val list = scala.collection.mutable.SortedSet[String]()

    if (and != null) list += and
    if (or != null) list += or

    s"WHERE ${list.mkString(filter.concatOperator.getOrElse(" AND "))}"
  }

  def getExpression(exp: Expression, table: String): String = {
    val value = if (exp.value.length > 1) exp.value.mkString("('", "', '", "')") else s"'${exp.value.head}'"
    val and = exp.and.map(e => " AND " + getExpression(e, table)).getOrElse("")
    val or = exp.or.map(e => " OR " + getExpression(e, table)).getOrElse("")
    s"$table.${exp.field} ${exp.operator} $value $and $or"
  }

//  def convertRowToJSON(row: Row): JSONObject = {
//    val m = row.getValuesMap(row.schema.fieldNames)
//    JSONObject(m)
//  }

}
