aws-kinesis-scala
========

[![Join the chat at https://gitter.im/bizreach/aws-kinesis-scala](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/bizreach/aws-kinesis-scala?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Scala client for Amazon Kinesis

## How to use

Add a following dependency into your `build.sbt` at first.

```scala
libraryDependencies += "jp.co.bizreach" %% "aws-kinesis-scala" % "0.0.1"
```

Then you can access Kinesis as following:

```scala
import jp.co.bizreach.kinesis._
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider

val credentialsProvider = new ClasspathPropertiesFileCredentialsProvider()
val kinesisClient = AmazonKinesisClient(credentialsProvider)

val putRecordRequest = new PutRecordRequest(
  streamName   = "streamName",
  partitionKey = "partitionKey",
  data         = "data".getBytes("UTF-8")
)

kinesisClient.putRecord(putRecordRequest)
```
