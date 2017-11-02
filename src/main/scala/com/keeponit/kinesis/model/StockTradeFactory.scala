package com.keeponit.kinesis.model

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}

object StockTradeFactory {
  private val JSON: ObjectMapper = new ObjectMapper()
  JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def fromJsonAsString(json: String): StockTrade = {
    JSON.readValue[StockTrade](json, classOf[StockTrade])
  }

  def fromJsonAsBytes(bytes: Array[Byte]): StockTrade = {
    JSON.readValue[StockTrade](bytes, classOf[StockTrade])
  }
}
