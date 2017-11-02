package com.keeponit.kinesis.processor

//import com.hypstar.kinesis.TradeType.TradeType

import com.keeponit.kinesis.model.StockTrade

import scala.collection.mutable

/**
  * Maintains running statistics of stock trades passed to it.
  *
  */
class StockStats {
  // Keeps count of trades for each ticker symbol for each trade type
  val countsByTradeType: mutable.Map[String, mutable.Map[String, Long]] =
    mutable.Map[String, mutable.Map[String, Long]]("BUY" -> mutable.HashMap[String, Long](), "SELL" -> mutable.HashMap[String, Long]())

  // Keeps the ticker symbol for the most popular stock for each trade type
  var mostPopularByTradeType: mutable.Map[String, String] = mutable.Map[String, String]()

  /**
    * Updates the statistics taking into account the new stock trade received.
    *
    * @param trade Stock trade instance
    */
  def addStockTrade(trade: StockTrade): Unit = { // update buy/sell count
    val counts = countsByTradeType(trade.getTradeType)
    var count = counts.getOrElse(trade.getTickerSymbol, 0L) // counts.get(trade.getTickerSymbol)
    // if (count == null  || count.isEmpty) count = 0L else count = count.get

    counts.put(trade.getTickerSymbol, {
      count += 1; count
    })

    // update most popular stock
    val mostPopular = mostPopularByTradeType.get(trade.getTradeType)

    if (mostPopular.isEmpty || mostPopular == null || countsByTradeType(trade.getTradeType)(mostPopular.get) < count)
      mostPopularByTradeType.put(trade.getTradeType, trade.getTickerSymbol)
  }

  override def toString: String =
    s"Most popular stock being bought: ${getMostPopularStock("BUY")}, ${getMostPopularStockCount("BUY")} buys. " +
      s"Most popular stock being sold:   ${getMostPopularStock("SELL")}, ${getMostPopularStockCount("SELL")} sells."

  private def getMostPopularStock(tradeType: String) = mostPopularByTradeType.getOrElse(tradeType, "???")

  private def getMostPopularStockCount(tradeType: String) = {
    val mostPopular = getMostPopularStock(tradeType)
    countsByTradeType(tradeType).getOrElse(mostPopular, 0L)
  }
}
