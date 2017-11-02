package com.keeponit.kinesis.model

import java.io.IOException

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
//import com.hypstar.kinesis.TradeType.TradeType

case class StockTrade(
                       @JsonProperty("tickerSymbol") tickerSymbol: String,
                       @JsonProperty("tradeType")tradeType: String,  // TODO: change back to TradeType enum type
                       @JsonProperty("price") price: Double,
                       @JsonProperty("quantity") quantity: Long,
                       @JsonProperty("id") id: Long) {
  private val JSON: ObjectMapper = new ObjectMapper()
  JSON.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

  def getTickerSymbol: String = tickerSymbol
  def getTradeType: String = tradeType // TradeType = if (tradeType == "BUY") TradeType.BUY else TradeType.SELL
  def getPrice: Double = price
  def getQuantity: Long = quantity
  def getId: Long = id

  def toJson: String = JSON.writeValueAsString(this)

  def toJsonAsBytes: Array[Byte] = try
    JSON.writeValueAsBytes(this)
  catch {
    case e: IOException =>
      println("Could not write JSON, error: " + e.getMessage)
      null
  }

  override def toString: String = s"ID $id: $tradeType $quantity shares of $tickerSymbol for $price"
}

