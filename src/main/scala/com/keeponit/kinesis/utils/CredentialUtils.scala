package com.keeponit.kinesis.utils

import com.amazonaws.AmazonClientException
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.profile.ProfileCredentialsProvider

/**
  * Provides utilities for retrieving credentials to talk to AWS
  */
object CredentialUtils {
  @throws[Exception]
  def getCredentialsProvider: AWSCredentialsProvider = {
    /*
    * The ProfileCredentialsProvider will return your [default] credential profile by
    * reading from the credentials file located at (~/.aws/credentials).
    */

    var credentialsProvider: AWSCredentialsProvider = null
    try
      credentialsProvider = new ProfileCredentialsProvider("stock")
    catch {
      case e: Exception =>
        throw new AmazonClientException("Cannot load the credentials from the credential profiles file. " + "Please make sure that your credentials file is at the correct " + "location (~/.aws/credentials), and is in valid format.", e)
    }
    credentialsProvider
  }
}