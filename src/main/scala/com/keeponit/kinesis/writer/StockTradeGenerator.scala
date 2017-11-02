package com.keeponit.kinesis.writer

import java.util
import java.util.Random
import java.util.concurrent.atomic.AtomicLong

import com.keeponit.kinesis.model.{StockPrice, StockTrade}

class StockTradeGenerator {
  private val STOCK_PRICES = new util.ArrayList[StockPrice]

  STOCK_PRICES.add(new StockPrice("AAPL", 119.72))
  STOCK_PRICES.add(new StockPrice("XOM", 91.56))
  STOCK_PRICES.add(new StockPrice("GOOG", 527.83))
  STOCK_PRICES.add(new StockPrice("BRK.A", 223999.88))
  STOCK_PRICES.add(new StockPrice("MSFT", 42.36))
  STOCK_PRICES.add(new StockPrice("WFC", 54.21))
  STOCK_PRICES.add(new StockPrice("JNJ", 99.78))
  STOCK_PRICES.add(new StockPrice("WMT", 85.91))
  STOCK_PRICES.add(new StockPrice("CHL", 66.96))
  STOCK_PRICES.add(new StockPrice("GE", 24.64))
  STOCK_PRICES.add(new StockPrice("NVS", 102.46))
  STOCK_PRICES.add(new StockPrice("PG", 85.05))
  STOCK_PRICES.add(new StockPrice("JPM", 57.82))
  STOCK_PRICES.add(new StockPrice("RDS.A", 66.72))
  STOCK_PRICES.add(new StockPrice("CVX", 110.43))
  STOCK_PRICES.add(new StockPrice("PFE", 33.07))
  STOCK_PRICES.add(new StockPrice("FB", 74.44))
  STOCK_PRICES.add(new StockPrice("VZ", 49.09))
  STOCK_PRICES.add(new StockPrice("PTR", 111.08))
  STOCK_PRICES.add(new StockPrice("BUD", 120.39))
  STOCK_PRICES.add(new StockPrice("ORCL", 43.40))
  STOCK_PRICES.add(new StockPrice("KO", 41.23))
  STOCK_PRICES.add(new StockPrice("T", 34.64))
  STOCK_PRICES.add(new StockPrice("DIS", 101.73))
  STOCK_PRICES.add(new StockPrice("AMZN", 370.56))

  /** The ratio of the deviation from the mean price **/
  private val MAX_DEVIATION = 0.2 // ie 20%

  /** The number of shares is picked randomly between 1 and the MAX_QUANTITY **/
  private val MAX_QUANTITY = 10000

  /** Probability of trade being a sell **/
  private val PROBABILITY_SELL = 0.4 // ie 40%

  private val random = new Random
  private val id = new AtomicLong(1)

  /**
    * Return a random stock trade with a unique id every time.
    *
    */
  def getRandomTrade: StockTrade = { // pick a random stock
    val stockPrice = STOCK_PRICES.get(random.nextInt(STOCK_PRICES.size))
    // pick a random deviation between -MAX_DEVIATION and +MAX_DEVIATION
    val deviation = (random.nextDouble - 0.5) * 2.0 * MAX_DEVIATION
    // set the price using the deviation and mean price
    var price = stockPrice.price * (1 + deviation)
    // round price to 2 decimal places
    price = price * 100.0.round / 100.0
    // set the trade type to buy or sell depending on the probability of sell
    var tradeType = "BUY"  // TradeType.BUY
    if (random.nextDouble < PROBABILITY_SELL) tradeType = "SELL" //TradeType.SELL
    // randomly pick a quantity of shares
    val quantity = random.nextInt(MAX_QUANTITY) + 1 // add 1 because nextInt() will return between 0 (inclusive)
    // and MAX_QUANTITY (exclusive). we want at least 1 share.
    StockTrade(stockPrice.tickerSymbol, tradeType, price, quantity, id.getAndIncrement)
  }
}
