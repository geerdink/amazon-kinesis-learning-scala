package com.keeponit.kinesis.utils

import com.amazonaws.ClientConfiguration

object ConfigurationUtils {
  private val APPLICATION_NAME = "hypstar-amazon-kinesis-learning"
  private val VERSION = "1.0.0"

  def getClientConfigWithUserAgent: ClientConfiguration = {
    val config: ClientConfiguration = new ClientConfiguration()
    val userAgent = new StringBuilder(ClientConfiguration.DEFAULT_USER_AGENT)
    // Separate fields of the user agent with a space
    userAgent.append(" ")
    // Append the application name followed by version number of the sample
    userAgent.append(APPLICATION_NAME)
    userAgent.append("/")
    userAgent.append(VERSION)
    config.setUserAgent(userAgent.toString)
    config
  }
}
