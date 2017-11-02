package com.keeponit.kinesis.processor

import com.amazonaws.services.kinesis.clientlibrary.exceptions.{InvalidStateException, ShutdownException, ThrottlingException}
import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.ShutdownReason
import com.amazonaws.services.kinesis.model.Record
import com.keeponit.kinesis.model.StockTradeFactory
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

class StockTradeRecordProcessor extends IRecordProcessor {
  private val LOG = LoggerFactory.getLogger(this.getClass)
  private var kinesisShardId: String = _

  // Reporting interval
  private val REPORTING_INTERVAL_MILLIS: Long = 5000L // 30 seconds
  private var nextReportingTimeInMillis: Long = 0L

  // Checkpointing interval
  private val CHECKPOINT_INTERVAL_MILLIS: Long = 60000L
  private var nextCheckpointTimeInMillis: Long = 0L

  // Aggregates stats for stock trades
  private var stockStats: StockStats = new StockStats

  def initialize(shardId: String): Unit = {
    //LOG.info("Initializing record processor for shard: " + shardId)
    this.kinesisShardId = shardId
    nextReportingTimeInMillis = System.currentTimeMillis + REPORTING_INTERVAL_MILLIS
    nextCheckpointTimeInMillis = System.currentTimeMillis + CHECKPOINT_INTERVAL_MILLIS
  }

  def processRecords(records: java.util.List[Record], checkpointer: IRecordProcessorCheckpointer): Unit = {
    // TODO: records.asScala
    // process record
    for (record <- records.asScala) processRecord(record)

    // If it is time to report stats as per the reporting interval, report stats
    if (System.currentTimeMillis > nextReportingTimeInMillis) {
      reportStats()
      resetStats()
      nextReportingTimeInMillis = System.currentTimeMillis + REPORTING_INTERVAL_MILLIS
    }

    // Checkpoint once every checkpoint interval
    if (System.currentTimeMillis > nextCheckpointTimeInMillis) {
      checkpoint(checkpointer)
      nextCheckpointTimeInMillis = System.currentTimeMillis + CHECKPOINT_INTERVAL_MILLIS
    }
  }

  private def reportStats(): Unit = {
    println("****** Shard " + kinesisShardId + " stats for last 1 minute ******\n" + stockStats + "\n" + "****************************************************************\n")
  }

  private def resetStats(): Unit = {
    stockStats = new StockStats
  }

  private def processRecord(record: Record): Unit = {
    try {
      val json = new String(record.getData.array)
      val trade = StockTradeFactory.fromJsonAsString(json)
      if (trade == null) throw new NullPointerException("Failed to parse json")
      LOG.info(s"Received trade ${record.getSequenceNumber}: ${trade.toString}")

      stockStats.addStockTrade(trade)
    }
    catch {
      case e: Exception => LOG.error(s"Unable to parse JSON for trade  ${record.getSequenceNumber} on key ${record.getPartitionKey}. Error: " + e.getMessage)
    }
  }

  override def shutdown(checkpointer: IRecordProcessorCheckpointer, reason: ShutdownReason): Unit = {
    LOG.warn("Shutting down record processor for shard: " + kinesisShardId)
    // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
    if (reason == ShutdownReason.TERMINATE) checkpoint(checkpointer)
  }

  private def checkpoint(checkpointer: IRecordProcessorCheckpointer): Unit = {
    LOG.info("Checkpointing shard " + kinesisShardId)
    try
      checkpointer.checkpoint()
    catch {
      case se: ShutdownException =>
        // Ignore checkpoint if the processor instance has been shutdown (fail over).
        LOG.info("Caught shutdown exception, skipping checkpoint: " + se.getMessage)
      case e: ThrottlingException =>
        // Skip checkpoint when throttled. In practice, consider a backoff and retry policy.
        LOG.error("Caught throttling exception, skipping checkpoint: " + e.getMessage)
      case e: InvalidStateException =>
        // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
        LOG.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library: " + e.getMessage)
    }
  }
}
