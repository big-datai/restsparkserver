package com.mynt.routes


import org.json4s.{DefaultFormats, Formats}

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
                       field: Option[String]
                       , operator: Option[String]
                       , value: Option[Array[String]]
                       , or: Option[Seq[Expression]]
                       , and: Option[Seq[Expression]]
                     )

trait Protocols extends DefaultFormats {

//  implicit val formats = DefaultFormats
//  implicit val joinFormat: Formats[Join] = jsonFormat4(Join.apply)
//  implicit val filterFormat: JsonFormat[Filter] = lazyFormat(jsonFormat(Filter, "and", "or", "concatOperator"))
//  implicit val expressionFormat: JsonFormat[Expression] = lazyFormat(jsonFormat(Expression
//    , "field"
//    , "operator"
//    , "value"
//    , "or"
//    , "and"))
//  implicit val queryFormat: RootJsonFormat[Query] = jsonFormat7(Query.apply)
}

trait Service extends Protocols {

  def fetchExampleQeury(): Future[Either[String, Query]] = {
    val join = Join("events", Some("inner"), "profile_id", Some("profile_id"))
    val group = Some(Seq("profiles.purchases"))
    val expressionAnd = List(
      Expression(Some("first"), Some(">="), Some(Array("2017-01-01")), None, Some(Seq(Expression(Some("inner first"), Some(">="), Some(Array("2017-02-02")), None, None))))
      , Expression(Some("last"), Some("<="), Some(Array("2017-10-10")), None, None)
      , Expression(Some("account_id"), Some("="), Some(Array("7")), None, None)
    )
//    val expressionOr = Expression(Some("gender"), Some("="), Some(Array("m")),
//      Some(Seq(Expression(Some("age_from"), Some("="), Some(Array("1"))), Some(Expression(Some("age_to"), Some("="), Some(Array("10")), None, None)), None))), None)
    val filter = Filter(Some(expressionAnd), None, Some("OR"))
    Future.successful(
      Right(
        Query("profiles", List("profiles.purchases", "COUNT(profiles.purchases)")
          , join = Some(join)
          , filter = Some(filter)
          , groupBy = group
          , orderBy = None
          , limit = Some(100)
        )
      )
    )
  }
}