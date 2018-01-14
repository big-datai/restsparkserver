package com.mynt.routes

import spray.json.{DefaultJsonProtocol, JsonFormat, RootJsonFormat}

import scala.concurrent.Future

case class Query(
                  table: String
                  , select: Seq[String]
                  , join: Option[Join]
                  , filter: Option[Filter]
                  , groupBy: Option[Seq[String]]
                  , orderBy: Option[Seq[String]]
                  , limit: Option[Int]
                )

case class Filter(and: Option[Seq[Expression]]
                  , or: Option[Seq[Expression]]
                  , concatOperator: Option[String] = Some(" AND "))

case class Join(
                 table: String
                 , joinType: Option[String]
                 , columnOn: String
                 , columnTo: Option[String]
               )

case class Expression(
                       field: String
                       , operator: String
                       , value: Array[String]
                       , or: Option[Expression]
                       , and: Option[Expression]
                     )

trait Protocols extends DefaultJsonProtocol {

  implicit val joinFormat: JsonFormat[Join] = jsonFormat4(Join.apply)
  implicit val filterFormat: JsonFormat[Filter] = lazyFormat(jsonFormat(Filter, "and", "or", "concatOperator"))
  implicit val expressionFormat: JsonFormat[Expression] = lazyFormat(jsonFormat(Expression
    , "field"
    , "operator"
    , "value"
    , "or"
    , "and"))
  implicit val queryFormat: RootJsonFormat[Query] = jsonFormat6(Query.apply)
}

trait Service extends Protocols {

  def fetchExampleQeury(): Future[Either[String, Query]] = {
    val join = Join("events", Some("inner"), "profile_id", Some("profile_id"))
    val group = Some(Seq("profiles.purchases"))
    val expressionAnd = List(
      Expression("first", ">=", Array("2017-01-01"), None, None)
      , Expression("last", "<=", Array("2017-10-10"), None, None)
      , Expression("account_id", "=", Array("7"), None, None)
    )
    val expressionOr = Expression("gender", "=", Array("m"),
      Some(Expression("age_from", "=", Array("1"), Some(Expression("age_to", "=", Array("10"), None, None)), None)), None)
    val filter = Filter(Some(expressionAnd), Some(List(expressionOr)), Some("OR"))
    Future.successful(Right(Query("profiles", List("profiles.purchases", "COUNT(profiles.purchases)"), Some(join), Some(filter), group, Some(100))))
  }
}