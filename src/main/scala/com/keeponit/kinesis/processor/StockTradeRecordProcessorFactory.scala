package com.keeponit.kinesis.processor

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory

class StockTradeRecordProcessorFactory extends IRecordProcessorFactory {
  def createProcessor = new StockTradeRecordProcessor()
}
