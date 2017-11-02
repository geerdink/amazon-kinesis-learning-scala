# Amazon Kinesis learning kit and connection framework for Scala applications
## Example: stock trades writer/processor

This tool is an example of using Amazon Kinesis in a Scala application.
The code is converted from the https://github.com/awslabs/amazon-kinesis-learning framework.

There is a _writer_  to publish events on the Kinesis bus.
There is a _processor_ to read events from the Kinesis bus.

Prerequisites:
* make sure you have your AWS credentials stored in ~/.aws/credentials.
* follow the steps on http://docs.aws.amazon.com/streams/latest/dev/learning-kinesis.html to set up your environment

To run:
* Writer (producer): 
    * StockTradesWriter _stream_name_ _region_
    * e.g. StockTradesWriter kinesis_test1 eu-central-1
* Processor (consumer):
    * StockTradesProcessor _application_name_ _stream_name_ _region_
    * e.g. StockTradesWriter my-first-kinesis-app kinesis_test1 eu-central-1
