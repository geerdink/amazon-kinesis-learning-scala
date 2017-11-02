package com.keeponit.kinesis.processor

import java.util.UUID

import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.{KinesisClientLibConfiguration, Worker}
import com.keeponit.kinesis.utils.{ConfigurationUtils, CredentialUtils}
import org.slf4j.{Logger, LoggerFactory}

object StockTradesProcessor {
  val LOG: Logger = LoggerFactory.getLogger(this.getClass)

  private def checkUsage(args: Array[String]): Unit = {
    if (args.length != 3) {
      System.err.println("Usage: " + this.getClass.getSimpleName + " <application name> <stream name> <region>")
      System.exit(1)
    }
  }

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    checkUsage(args)
    val applicationName = args(0)
    val streamName = args(1)
    val region = RegionUtils.getRegion(args(2))
    if (region == null) {
      System.err.println(args(2) + " is not a valid AWS region.")
      System.exit(1)
    }

    LOG.info("##### Starting Stock Trades Processor - Connecting to Amazon Kinesis #####")

    val credentialsProvider = CredentialUtils.getCredentialsProvider
    val workerId = String.valueOf(UUID.randomUUID)
    val kclConfig = new KinesisClientLibConfiguration(applicationName, streamName, credentialsProvider, workerId)
      .withRegionName(region.getName)
      .withCommonClientConfig(ConfigurationUtils.getClientConfigWithUserAgent)

    val recordProcessorFactory = new StockTradeRecordProcessorFactory

    // Create the KCL worker with the stock trade record processor factory
    val worker = new Worker(recordProcessorFactory, kclConfig)

    LOG.info(s"Created worker with application name: ${worker.getApplicationName}")

    val t = new Thread(new Runnable{
      override def run(): Unit = {
        while (!Thread.currentThread.isInterrupted) {
          worker.run()
        }
      }
    })

    sys.addShutdownHook({
      LOG.warn("Interrupted. Shutting down worker...")
      val shutdown = worker.startGracefulShutdown()
        while (!shutdown.isDone) {
          //LOG.info("Waiting for shutdown to complete...")
          Thread.sleep(250)
        }

      t.interrupt()
      LOG.info("Done.")
    })

    t.start()
  }
}
