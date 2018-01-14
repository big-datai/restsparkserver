package com.mynt.databases.entity


case class Profiles(
                     profile_id: String //7data_id
                     , activity_first: java.sql.Date
                     , activity_last: java.sql.Date
                     , myn_ids: Array[String]
                     , gender: String
                     , age_from: Long
                     , age_to: Long
                     , appnexus_ids: Array[String]
                     , enriched_at: java.sql.Date
                     , account_id: Long
                     , vertical: Array[Long]
                     , sub_vertical: Array[Long]
                     , purchase: Array[Long]
                     , sub_purchase: Array[Long]
                     , datasource_ids: Array[String]
                     , campaignInteraction: Array[Long]
                     , campaignExposure: Array[Long]
                     , lineitem_ids: Long
                     , creative_ids: Long
                   )

case class Profiles2(
                     profile_id: String //7data_id
                     , myn_ids: Array[String]
                     , lineitem_ids: Long
                     , creative_ids: Long
                   )

case class Events(
                   source: String
                   , sourceid: String
                   , domain: String
                   , path: String
                   , atype: String
                   , createdat: java.sql.Date
                   , a7dataid: String
                   , referrer: String
                   , document_referrer: String
                   , url: String
                   , gender: String
                   , account_id: String
                   , authenticated: Long
                   , loyalty: Long
                   , agefrom: Long
                   , ageto: Long
                   , os_name: String
                   , os_version: String
                   , location_lat: String
                   , location_lng: String
                   , location_country: String
                   , location_city: String
                   , location_region: String
                   , key: String
                   , device_type: String
                   , device_brand: String
                   , device_model: String
                   , device_browser: String
                   , verticals: Array[Long]
                   , purchases: Array[Long]
                   , sub_purchases: Array[Long]
                   , stamp: Long
                 )