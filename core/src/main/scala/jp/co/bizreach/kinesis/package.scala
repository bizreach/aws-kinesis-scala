package jp.co.bizreach

import java.nio.ByteBuffer

import com.amazonaws.services.kinesis.model.{
  AddTagsToStreamRequest => AWSAddTagsToStreamRequest,
  CreateStreamRequest => AWSCreateStreamRequest,
  DeleteStreamRequest => AWSDeleteStreamRequest,
  DescribeStreamRequest => AWSDescribeStreamRequest,
  DescribeStreamResult => AWSDescribeStreamResult,
  GetRecordsRequest => AWSGetRecordsRequest,
  GetRecordsResult => AWSGetRecordsResult,
  GetShardIteratorRequest => AWSGetShardIteratorRequest,
  GetShardIteratorResult => AWSGetShardIteratorResult,
  ListStreamsRequest => AWSListStreamsRequest,
  ListStreamsResult => AWSListStreamsResult,
  ListTagsForStreamRequest => AWSListTagsForStreamRequest,
  ListTagsForStreamResult => AWSListTagsForStreamResult,
  MergeShardsRequest => AWSMergeShardsRequest,
  PutRecordRequest => AWSPutRecordRequest,
  PutRecordResult => AWSPutRecordResult,
  PutRecordsRequest => AWSPutRecordsRequest,
  PutRecordsRequestEntry => AWSPutRecordsRequestEntry,
  PutRecordsResult => AWSPutRecordsResult,
  RemoveTagsFromStreamRequest => AWSRemoveTagsFromStreamRequest,
  SplitShardRequest => AWSSplitShardRequest}

import com.amazonaws.metrics.RequestMetricCollector

import scala.collection.JavaConverters._
import scala.language.implicitConversions

package object kinesis {

  @deprecated("Use type `AmazonKinesis` instead of `AmazonKinesisClient`", "0.0.6")
  type AmazonKinesisClient = AmazonKinesis
  @deprecated("Use object `AmazonKinesis` instead of `AmazonKinesisClient`", "0.0.6")
  val AmazonKinesisClient = AmazonKinesis
  @deprecated("Use type `AmazonKinesisAsync` instead of `AmazonKinesisAsyncClient`", "0.0.6")
  type AmazonKinesisAsyncClient = AmazonKinesisAsync
  @deprecated("Use object `AmazonKinesisAsync` instead of `AmazonKinesisAsyncClient`", "0.0.6")
  val AmazonKinesisAsyncClient = AmazonKinesisAsync
  @deprecated("Use type `BufferedAmazonKinesis` instead of `BufferedAmazonKinesisClient`", "0.0.6")
  type BufferedAmazonKinesisClient = BufferedAmazonKinesis
  @deprecated("Use object `BufferedAmazonKinesis` instead of `BufferedAmazonKinesisClient`", "0.0.6")
  val BufferedAmazonKinesisClient = BufferedAmazonKinesis


  private[this] implicit class JListConverters[A](list: java.util.List[A]) {
    def immutableSeq: Seq[A] = list.asScala.toSeq
  }

  case class AddTagsToStreamRequest(streamName: String,
                                    tags: Map[String, String],
                                    requestMetricCollector: Option[RequestMetricCollector] = None)

  implicit def convertAddTagsToStreamRequest(request: AddTagsToStreamRequest): AWSAddTagsToStreamRequest = {
    val awsRequest = new AWSAddTagsToStreamRequest()
    awsRequest.setStreamName(request.streamName)
    awsRequest.setTags(request.tags.asJava)
    request.requestMetricCollector.foreach(awsRequest.setRequestMetricCollector)
    awsRequest
  }

  case class CreateStreamRequest(streamName: String,
                                 shardCount: Int,
                                 requestMetricCollector: Option[RequestMetricCollector] = None)

  implicit def convertCreateStreamRequest(request: CreateStreamRequest): AWSCreateStreamRequest = {
    val awsRequest = new AWSCreateStreamRequest()
    awsRequest.setStreamName(request.streamName)
    awsRequest.setShardCount(request.shardCount)
    request.requestMetricCollector.foreach(awsRequest.setRequestMetricCollector)
    awsRequest
  }

  case class DeleteStreamRequest(streamName: String,
                                 requestMetricCollector: Option[RequestMetricCollector] = None)

  implicit def convertDeleteStreamRequest(request: DeleteStreamRequest): AWSDeleteStreamRequest = {
    val awsRequest = new AWSDeleteStreamRequest()
    awsRequest.setStreamName(request.streamName)
    request.requestMetricCollector.foreach(awsRequest.setRequestMetricCollector)
    awsRequest
  }

  case class DescribeStreamRequest(streamName: String,
                                   limit: Option[Int] = None,
                                   exclusiveStartShardId: Option[String] = None,
                                   requestMetricCollector: Option[RequestMetricCollector] = None)

  implicit def convertDescribeStreamRequest(request: DescribeStreamRequest): AWSDescribeStreamRequest = {
    val awsRequest = new AWSDescribeStreamRequest()
    awsRequest.setStreamName(request.streamName)
    request.limit.foreach { limit =>
      awsRequest.setLimit(limit)
    }
    request.exclusiveStartShardId.foreach(awsRequest.setExclusiveStartShardId)
    request.requestMetricCollector.foreach(awsRequest.setRequestMetricCollector)
    awsRequest
  }

  case class DescribeStreamResult(streamDescription: StreamDescription)
  case class StreamDescription(streamName: String, streamARN: String, streamStatus: String, shards: Seq[Shard], hasMoreShards: Boolean)
  case class Shard(shardId: String, parentShardId: String, adjacentParentShardId: String, hashKeyRange: HashKeyRange, sequenceNumberRange: SequenceNumberRange)
  case class HashKeyRange(startingHashKey: String, endingHashKey: String) // TODO BitInt?
  case class SequenceNumberRange(startingSequenceNumber: String, endingSequenceNumber: String) // TODO BigInt?

  implicit def convertDescribeStreamResult(result: AWSDescribeStreamResult): DescribeStreamResult = {
    DescribeStreamResult(
      StreamDescription(
        streamName = result.getStreamDescription.getStreamName,
        streamARN = result.getStreamDescription.getStreamARN,
        streamStatus = result.getStreamDescription.getStreamStatus,
        shards = result.getStreamDescription.getShards.immutableSeq.map { shard =>
          Shard(
            shardId = shard.getShardId,
            parentShardId = shard.getParentShardId,
            adjacentParentShardId = shard.getAdjacentParentShardId,
            hashKeyRange = HashKeyRange(
              startingHashKey = shard.getHashKeyRange.getStartingHashKey,
              endingHashKey = shard.getHashKeyRange.getEndingHashKey
            ),
            sequenceNumberRange = SequenceNumberRange(
              startingSequenceNumber = shard.getSequenceNumberRange.getStartingSequenceNumber,
              endingSequenceNumber = shard.getSequenceNumberRange.getEndingSequenceNumber
            )
          )
        },
        hasMoreShards = result.getStreamDescription.getHasMoreShards
      )
    )
  }

  case class GetRecordsRequest(shardIterator: String,
                               limit: Option[Int] = None,
                               requestMetricCollector: Option[RequestMetricCollector] = None)

  implicit def convertGetRecordsRequest(request: GetRecordsRequest): AWSGetRecordsRequest = {
    val awsRequest = new AWSGetRecordsRequest()
    awsRequest.setShardIterator(request.shardIterator)
    request.limit.foreach { limit =>
      awsRequest.setLimit(limit)
    }
    request.requestMetricCollector.foreach(awsRequest.setRequestMetricCollector)
    awsRequest
  }

  case class GetRecordsResult(records: Seq[Record], nextShardIterator: String, millisBehindLatest: Long)
  case class Record(data: Array[Byte], sequenceNumber: String, partitionKey: String)

  implicit def convertGetRecordsResult(result: AWSGetRecordsResult): GetRecordsResult = {
    GetRecordsResult(
      records = result.getRecords.immutableSeq.map { record =>
        Record(
          data           = record.getData.array(),
          sequenceNumber = record.getSequenceNumber,
          partitionKey   = record.getPartitionKey
        )
      },
      nextShardIterator = result.getNextShardIterator,
      millisBehindLatest = result.getMillisBehindLatest
    )
  }

  case class GetShardIteratorRequest(streamName: String,
                                     shardId: String,
                                     shardIteratorType: Option[String] = None,
                                     requestMetricCollector: Option[RequestMetricCollector] = None)

  implicit def convertGetShardIteratorRequest(request: GetShardIteratorRequest): AWSGetShardIteratorRequest = {
    val awsRequest = new AWSGetShardIteratorRequest()
    awsRequest.setStreamName(request.streamName)
    awsRequest.setShardId(request.shardId)
    request.shardIteratorType.foreach(awsRequest.setShardIteratorType)
    request.requestMetricCollector.foreach(awsRequest.setRequestMetricCollector)
    awsRequest
  }

  case class GetShardIteratorResult(shardIterator: String)

  implicit def convertGetShardIteratorResult(result: AWSGetShardIteratorResult): GetShardIteratorResult = {
    GetShardIteratorResult(result.getShardIterator)
  }

  case class ListStreamsRequest(limit: Option[Int] = None,
                                exclusiveStartStreamName: Option[String] = None,
                                requestMetricCollector: Option[RequestMetricCollector] = None)

  implicit def convertListStreamsRequest(request: ListStreamsRequest): AWSListStreamsRequest = {
    val awsRequest = new AWSListStreamsRequest()
    request.limit.foreach { limit =>
      awsRequest.setLimit(limit)
    }
    request.exclusiveStartStreamName.foreach(awsRequest.setExclusiveStartStreamName)
    request.requestMetricCollector.foreach(awsRequest.setRequestMetricCollector)
    awsRequest
  }

  case class ListStreamsResult(streamNames: Seq[String], hasMoreStreams: Boolean)

  implicit def convertListStreamsResult(request: AWSListStreamsResult): ListStreamsResult = {
    ListStreamsResult(
      streamNames = request.getStreamNames.immutableSeq,
      hasMoreStreams = request.getHasMoreStreams
    )
  }

  case class ListTagsForStreamRequest(streamName: String,
                                      exclusiveStartTagKey: Option[String] = None,
                                      limit: Option[Int] = None,
                                      requestMetricCollector: Option[RequestMetricCollector] = None)

  implicit def convertListTagsForStreamRequest(request: ListTagsForStreamRequest): AWSListTagsForStreamRequest = {
    val awsRequest = new AWSListTagsForStreamRequest()
    awsRequest.setStreamName(request.streamName)
    request.exclusiveStartTagKey.foreach(awsRequest.setExclusiveStartTagKey)
    request.limit.foreach { limit =>
      awsRequest.setLimit(limit)
    }
    request.requestMetricCollector.foreach(awsRequest.setRequestMetricCollector)
    awsRequest
  }

  case class ListTagsForStreamResult(tags: Seq[Tag], hasMoreTags: Boolean)
  case class Tag(key: String, value: String)

  implicit def convertListTagsForStreamResult(result: AWSListTagsForStreamResult): ListTagsForStreamResult = {
    ListTagsForStreamResult(
      tags = result.getTags.immutableSeq.map { tag =>
        Tag(key = tag.getKey, value = tag.getValue)
      },
      hasMoreTags = result.getHasMoreTags
    )
  }

  case class MergeShardsRequest(streamName: String,
                                shardToMerge: String,
                                adjacentShardToMerge: String,
                                requestMetricCollector: Option[RequestMetricCollector] = None)

  implicit def convertMergeShardsRequest(request: MergeShardsRequest): AWSMergeShardsRequest = {
    val awsRequest = new AWSMergeShardsRequest()
    awsRequest.setStreamName(request.streamName)
    awsRequest.setShardToMerge(request.shardToMerge)
    awsRequest.setAdjacentShardToMerge(request.adjacentShardToMerge)
    request.requestMetricCollector.foreach(awsRequest.setRequestMetricCollector)
    awsRequest
  }

  case class PutRecordRequest(streamName: String,
                              partitionKey: String,
                              data: Array[Byte],
                              explicitHashKey: Option[String] = None,
                              sequenceNumberForOrdering: Option[String] = None,
                              requestMetricCollector: Option[RequestMetricCollector] = None)

  implicit def convertPutRecordRequest(request: PutRecordRequest): AWSPutRecordRequest = {
    val awsRequest = new AWSPutRecordRequest()
    awsRequest.setStreamName(request.streamName)
    awsRequest.setData(ByteBuffer.wrap(request.data))
    awsRequest.setPartitionKey(request.partitionKey)
    request.explicitHashKey.foreach(awsRequest.setExplicitHashKey)
    request.sequenceNumberForOrdering.foreach(awsRequest.setSequenceNumberForOrdering)
    request.requestMetricCollector.foreach(awsRequest.setRequestMetricCollector)
    awsRequest
  }

  case class PutRecordResult(shardId: String, sequenceNumber: String)

  implicit def convertPutRecordResult(result: AWSPutRecordResult): PutRecordResult = {
    PutRecordResult(
      shardId = result.getShardId,
      sequenceNumber = result.getSequenceNumber
    )
  }

  val recordsMaxCount = 500
  val recordMaxDataSize = 1024 * 1024
  val recordsMaxDataSize = 1024 * 1024 * 5

  case class PutRecordsRequest(streamName: String,
                               records: Seq[PutRecordsEntry],
                               requestMetricCollector: Option[RequestMetricCollector] = None)

  case class PutRecordsEntry(partitionKey: String, data: Array[Byte], explicitHashKey: Option[String] = None){
    val recordSize = partitionKey.getBytes.length + data.length
  }

  implicit def convertPutRecordsRequest(request: PutRecordsRequest): AWSPutRecordsRequest = {
    val entries = request.records.map { entry =>
      val awsEntry = new AWSPutRecordsRequestEntry()
      awsEntry.setPartitionKey(entry.partitionKey)
      awsEntry.setData(ByteBuffer.wrap(entry.data))
      entry.explicitHashKey.foreach(awsEntry.setExplicitHashKey)
      awsEntry
    }

    val awsRequest = new AWSPutRecordsRequest()
    awsRequest.setStreamName(request.streamName)
    awsRequest.setRecords(entries.asJava)
    request.requestMetricCollector.foreach(awsRequest.setRequestMetricCollector)
    awsRequest
  }

  case class PutRecordsResult(failedRecordCount: Int, records: Seq[PutRecordsResultEntry])
  case class PutRecordsResultEntry(sequenceNumber: String, shardId: String, errorCode: String, errorMessage: String)

  implicit def convertPutRecordsResult(result: AWSPutRecordsResult): PutRecordsResult = {
    PutRecordsResult(
      failedRecordCount = result.getFailedRecordCount,
      records = result.getRecords.immutableSeq.map { record =>
        PutRecordsResultEntry(
          sequenceNumber = record.getSequenceNumber,
          shardId = record.getShardId,
          errorCode = record.getErrorCode,
          errorMessage = record.getErrorMessage
        )
      }
    )
  }

  case class RemoveTagsFromStreamRequest(streamName: String,
                                         tagKeys: Seq[String],
                                         requestMetricCollector: Option[RequestMetricCollector] = None)

  implicit def convertRemoveTagsFromStreamRequest(request: RemoveTagsFromStreamRequest): AWSRemoveTagsFromStreamRequest = {
    val awsRequest = new AWSRemoveTagsFromStreamRequest()
    awsRequest.setStreamName(request.streamName)
    awsRequest.setTagKeys(request.tagKeys.asJava)
    request.requestMetricCollector.foreach(awsRequest.setRequestMetricCollector)
    awsRequest
  }

  case class SplitShardRequest(streamName: String,
                               shardToSplit: String,
                               newStartingHashKey: String,
                               requestMetricCollector: Option[RequestMetricCollector] = None)

  implicit def convertSplitShardRequest(request: SplitShardRequest): AWSSplitShardRequest = {
    val awsRequest = new AWSSplitShardRequest()
    awsRequest.setStreamName(request.streamName)
    awsRequest.setShardToSplit(request.shardToSplit)
    awsRequest.setNewStartingHashKey(request.newStartingHashKey)
    request.requestMetricCollector.foreach(awsRequest.setRequestMetricCollector)
    awsRequest
  }

}
