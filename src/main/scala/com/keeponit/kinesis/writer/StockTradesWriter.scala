package com.keeponit.kinesis.writer

import java.nio.ByteBuffer

import com.amazonaws.AmazonClientException
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.model.{PutRecordRequest, ResourceNotFoundException}
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}
import com.keeponit.kinesis.model.StockTrade
import com.keeponit.kinesis.utils.{ConfigurationUtils, CredentialUtils}
import org.slf4j.{Logger, LoggerFactory}

object StockTradesWriter {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  private def checkUsage(args: Array[String]): Unit = {
    if (args.length != 2) {
      LOG.error("Usage: " + this.getClass.getSimpleName + " <stream name> <region>")
      System.exit(1)
    }
  }

  /**
    * Checks if the stream exists and is active
    *
    * @param kinesisClient Amazon Kinesis client instance
    * @param streamName    Name of stream
    */
  private def validateStream(kinesisClient: AmazonKinesis, streamName: String): Unit = {
    try {
      val result = kinesisClient.describeStream(streamName)
      if (!("ACTIVE" == result.getStreamDescription.getStreamStatus)) {
        LOG.error("Stream " + streamName + " is not active. Please wait a few moments and try again.")
        System.exit(1)
      }
    } catch {
      case e: ResourceNotFoundException =>
        LOG.error("Stream " + streamName + " does not exist. Please create it in the console.")
        LOG.error(e.getMessage)
        System.exit(1)
      case e: Exception =>
        LOG.error("Error found while describing the stream " + streamName)
        LOG.error(e.getMessage)
        System.exit(1)
    }
  }

  /**
    * Uses the Kinesis client to send the stock trade to the given stream.
    *
    * @param trade         instance representing the stock trade
    * @param kinesisClient Amazon Kinesis client
    * @param streamName    Name of stream
    */
  private def sendStockTrade(trade: StockTrade, kinesisClient: AmazonKinesis, streamName: String): Unit = {
    LOG.info(s"Putting trade to Kinesis stream '$streamName': " + trade.toJson)

    val bytes = trade.toJsonAsBytes

    // The bytes could be null if there is an issue with the JSON serialization by the Jackson JSON library.
    if (bytes == null) {
      LOG.warn("Could not get JSON bytes for stock trade")
      return
    }

    val putRecord = new PutRecordRequest
    putRecord.setStreamName(streamName)

    // We use the ticker symbol as the partition key
    putRecord.setPartitionKey(trade.getTickerSymbol)
    putRecord.setData(ByteBuffer.wrap(bytes))
    try {
      kinesisClient.putRecord(putRecord)
    } catch {
      case ex: AmazonClientException =>
        LOG.error("Error sending record to Amazon Kinesis: " + ex.getMessage)
    }
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    checkUsage(args)
    val streamName = args(0)
    val regionName = args(1)

    val region = RegionUtils.getRegion(regionName)
    if (region == null) {
      LOG.error(regionName + " is not a valid AWS region.")
      System.exit(1)
    }

    val credentials = CredentialUtils.getCredentialsProvider.getCredentials
    val kinesisClient = new AmazonKinesisClient(credentials, ConfigurationUtils.getClientConfigWithUserAgent)
    kinesisClient.setRegion(region)

    // Validate that the stream exists and is active
    validateStream(kinesisClient, streamName)

    // Repeatedly send stock trades with a 500 milliseconds wait in between
    val stockTradeGenerator = new StockTradeGenerator
    while (true) {
      val trade = stockTradeGenerator.getRandomTrade
      sendStockTrade(trade, kinesisClient, streamName)
      Thread.sleep(500)
    }
  }
}
