package com.keeponit.kinesis.model

/**
  * Represents the type of the stock trade eg buy or sell.
  */
object TradeType extends Enumeration {
  type TradeType = Value
  val BUY, SELL = Value
}
