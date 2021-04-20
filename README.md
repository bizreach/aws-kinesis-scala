aws-kinesis-scala [![Build Status](https://travis-ci.org/bizreach/aws-kinesis-scala.svg?branch=master)](https://travis-ci.org/bizreach/aws-kinesis-scala)
========

Scala client for Amazon Kinesis with [Apache Spark](#apache-spark) support.

For Apache Spark, reading from Kinesis is supported by Spark Streaming Kinesis Integration, but it does not support writing to Kinesis. This library makes possible to write Spark's RDD and Spark Streaming's DStream to Kinesis.

## Installation

Add a following dependency into your `build.sbt`.

core only:
```scala
libraryDependencies += "jp.co.bizreach" %% "aws-kinesis-scala" % "0.0.12"
```

use spark integration:
```scala
libraryDependencies += "jp.co.bizreach" %% "aws-kinesis-spark" % "0.0.12"
```

## Usage

Create the `AmazonKinesis` at first.

```scala
import jp.co.bizreach.kinesis._

implicit val region = Regions.AP_NORTHEAST_1

// use DefaultAWSCredentialsProviderChain
val client = AmazonKinesis()

// specify an explicit Provider
val client = AmazonKinesis(new InstanceProfileCredentialsProvider())

// specify an explicit client configuration
val client = AmazonKinesis(new ClientConfiguration().withProxyHost("proxyHost"))

// both
val client = AmazonKinesis(
  new InstanceProfileCredentialsProvider(),
  new ClientConfiguration().withProxyHost("proxyHost")
)
```

Then you can access Kinesis as following:

```scala
val request = PutRecordRequest(
  streamName   = "streamName",
  partitionKey = "partitionKey",
  data         = "data".getBytes("UTF-8")
)

// not retry
client.putRecord(request)

// if failure, max retry count is 3 (SDK default)
client.putRecordWithRetry(request)
```

### Amazon Kinesis Firehose

Create the `AmazonKinesisFirehose` at first.

```scala
import jp.co.bizreach.kinesisfirehose._

implicit val region = Regions.US_EAST_1

// use DefaultAWSCredentialsProviderChain
val client = AmazonKinesisFirehose()

... as with kinesis ...
```

Then you can access Kinesis Firehose as following:

```scala
val request = PutRecordRequest(
  deliveryStreamName = "firehose-example",
  record             = "data".getBytes("UTF-8")
)

// not retry
client.putRecord(request)

// if failure, max retry count is 3 (SDK default)
client.putRecordWithRetry(request)
```

For batch processing to Kinesis Firehose:

```scala
val request = PutRecordBatchRequest(
  deliveryStreamName = "firehose-example",
  records             = Seq("data".getBytes("UTF-8"), "data".getBytes("UTF-8"))
)

// not retry
client.putRecordBatch(request)

// retry
client.putRecordBatchWithRetry(request)
```

## [Apache Spark][]

aws-kinesis-spark provides integration with Spark: for writing, methods that work on any `RDD`.

Import the `jp.co.bizreach.kinesis.spark._` to gain `saveToKinesis` method on your RDDs:

```scala
import jp.co.bizreach.kinesis.spark._

val rdd: RDD[Map[String, Option[Any]]] = ...

rdd.saveToKinesis(
  streamName = "streamName",
  region     = Regions.AP_NORTHEAST_1,
  chunk      = 30
)
```

You can also write data to Kinesis from Spark Streaming with DStreams.

```scala
import jp.co.bizreach.kinesis.spark._

val dstream: DStream[Map[String, Option[Any]]] = ...

dstream.foreachRDD { rdd =>
  rdd.saveToKinesis( ... )
}
```

[Apache Spark]: http://spark.apache.org
