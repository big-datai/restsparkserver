package com.mynt.routes

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._

import scala.util.Try
import scala.util.parsing.json.JSONObject
import org.json4s._
import org.json4s.native.JsonMethods._

import scala.collection.mutable
import scala.concurrent.ExecutionContextExecutor

import scala.language.postfixOps


trait RunQuerySevice extends LazyLogging {

  implicit val formats: DefaultFormats = DefaultFormats
  // Add two table to Map
  val tables = new mutable.HashMap[String, DataFrame]()

  implicit val system: ActorSystem = ActorSystem()
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  // needed for the future map/flatmap in the end
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  def convertRowToJSON(row: Row): String = {
    val m = row.getValuesMap(row.schema.fieldNames)
    JSONObject(m).toString()
  }

  def runQueryJSON(json: String): Try[Array[Byte]] = {
    val df = tables.head._2.sparkSession.sql(makeQueryBody(json))
    runQuery(df)
  }

  def runQuery(sql: String, spark: SparkSession): Try[Array[Byte]] = {
    runQuery(spark.sql(sql))
  }

  def runQuery(df: DataFrame): Try[Array[Byte]] = {
    Try(df.toJSON.collect().mkString("[", ",", "]").getBytes)
  }

  def makeQueryBody(json: String): String = {
    val data = parse(json)
    val table = getTable(data)
    val select = getSelect(data)
    val join = getJoin(data, table)
    val where = getWhere(data)
    val groupBy = getGroupBy(data)
    val orderBy = getOrderBy(data)
    val limit = getLimit(data)

    logger.debug(s"Query: $select $table $where $groupBy $orderBy $limit")
    s"$select $table $join $where $groupBy $orderBy $limit"
  }

  def getTable(json: JValue): String = {
    val table = json \ "table" match {
      case JString(s) => s
      case _ => throw new Throwable(s"JSON object doesn't contain table name definition")
    }

    tables.get(table) match {
      case Some(df) =>
        df.createOrReplaceTempView(table)
        table
      case _ => throw new Throwable(s"Cannot find table: $table")
    }
  }

  def getSelect(json: JValue): String = {
    val select = json \ "select"

    val selectParameters = select match {
      case JArray(x) => x.map(matchingValue(_, "At least one query parameter have to be present"))
      case _ => throw new Throwable("At least one query parameter have to be present")
    }

    selectParameters.mkString("SELECT ", ", ", " FROM")
  }

  def getGroupBy(json: JValue): String = {
    val groupBy = json \ "groupBy"

    groupBy match {
      case JArray(x) =>
        val xs = x.map(matchingValue(_, "At least one group by parameter has to be presented"))
        xs.mkString("GROUP BY ", ", ", "") //.agg(xs.head, xs.tail:_*)
      case _ => ""
    }
  }

  def getOrderBy(json: JValue): String = {
    val orderBy = json \ "orderBy"

    orderBy match {
      case JArray(x) =>
        val xs = x.map(matchingValue(_, "At least one order by parameter has to be presented"))
        xs.mkString("ORDER BY ", ", ", "") //df.orderBy(xs.head, xs.tail:_*)
      case _ => ""
    }
  }

  def getJoin(json: JValue, queryTable: String): String = {
    val join = (json \ "join").toOption

    join.map(optJoin => {
      val o = optJoin.extract[Join]

      tables.get(o.table) match {
        case Some(df) => df.createOrReplaceTempView(o.table)
        case None => throw new Throwable(s"Cannot match joining table ${o.table}")
      }

      s"${o.joinType.getOrElse("INNER")} JOIN ${o.table} ON ${o.table}.${o.columnOn} = $queryTable.${o.columnTo.getOrElse(o.columnOn)}"
    }).getOrElse("")

  }

  def getWhere(json: JValue): String = {
    val filter = json \ "filter"
    val andFilter = extractExpression(filter, "and")
    val orFilter = extractExpression(filter, "or")
    val concatOp = filter \ "concatOperator" match {
      case JString(s) => s
      case _ => "AND"
    }

    val result = new mutable.ListBuffer[String]()
    if (!andFilter.isEmpty) result += (andFilter)
    if (!orFilter.isEmpty) result += (orFilter)
    //val result = Seq(andFilter, orFilter).filter(_.asInstanceOf[List].nonEmpty)

    if (result.isEmpty) "" else result.mkString("WHERE (", s") $concatOp (", ")")
  }

  def getExpression(exp: Expression): String = {
    val builder: StringBuilder = new StringBuilder()
    val field = exp.field.getOrElse("")
    val oper = exp.operator.getOrElse("")
    val value = exp.value.getOrElse(Array())
    val v = if (value.length > 1) value.mkString("('", "', '", "')") else s"'${value.mkString}'"

    val andExpr = exp.and.map(_.map(getExpression).toSet.mkString(" (", " AND ", ")")).getOrElse("")
    val orExpr = exp.or.map(_.map(getExpression).toSet.mkString(" (", " OR ", ")")).getOrElse("")

    if ((field.isEmpty && !oper.isEmpty) || (!field.isEmpty && oper.isEmpty)) throw new Throwable("Field attribute is missed or filled improperly.")

    if (!field.isEmpty && !oper.isEmpty) {
      builder
        .append(field)
        .append(oper)
        .append(v)
    }

    builder
      .append(andExpr)
      .append(orExpr)
      .toString
  }

  def getLimit(json: JValue): String = {
    val limit = json \ "limit" toOption

    limit.map(l => s"LIMIT ${l.extract[Int]}").getOrElse("")
  }

  private def extractExpression(json: JValue, op: String): String = {
    val expression = json \ op toOption

    expression.map {
      e =>
        e match {
          case JArray(arr) => arr.map(e => getExpression(e.extract[Expression]))
          case _ => Nil
        }
    }.map(_.mkString(s" ${op.toUpperCase} ")).getOrElse("")
  }

  private def matchingValue(jValue: JValue, errorMsg: String = "") = {
    jValue match {
      case JString(s) => s
      case _ => throw new Throwable(errorMsg)
    }
  }
}
