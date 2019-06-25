package jp.co.bizreach

import java.nio.ByteBuffer

import com.amazonaws.services.kinesisfirehose.model.{
  PutRecordRequest => AWSPutRecordRequest,
  Record => AWSRecord,
  PutRecordResult => AWSPutRecordResult,
  PutRecordBatchRequest => AWSPutRecordBatchRequest,
  PutRecordBatchResult => AWSPutRecordBatchResult}

import scala.collection.JavaConverters._
import scala.language.implicitConversions

package object kinesisfirehose {

  private[this] implicit class JListConverters[A](list: java.util.List[A]) {
    def immutableSeq: Seq[A] = list.asScala.toSeq
  }

  case class PutRecordRequest(deliveryStreamName: String, record: Array[Byte])

  implicit def convertPutRecordRequest(request: PutRecordRequest): AWSPutRecordRequest = {
    val awsRequest = new AWSPutRecordRequest()
    awsRequest.setDeliveryStreamName(request.deliveryStreamName)
    awsRequest.setRecord(new AWSRecord().withData(ByteBuffer.wrap(request.record)))
    awsRequest
  }

  case class PutRecordResult(recordId: String)

  implicit def convertPutRecordResult(result: AWSPutRecordResult): PutRecordResult = {
    PutRecordResult(
      recordId = result.getRecordId
    )
  }

  case class PutRecordBatchRequest(deliveryStreamName: String, records: Seq[Array[Byte]])

  implicit def convertPutRecordBatchRequest(request: PutRecordBatchRequest): AWSPutRecordBatchRequest = {
    val awsRequest = new AWSPutRecordBatchRequest()
    awsRequest.setDeliveryStreamName(request.deliveryStreamName)
    awsRequest.setRecords(request.records.map { record =>
      new AWSRecord().withData(ByteBuffer.wrap(record))
    }.asJava)
    awsRequest
  }

  case class PutRecordBatchResult(failedPutCount: Int, records: Seq[PutRecordBatchResponseEntry])
  case class PutRecordBatchResponseEntry(recordId: String, errorCode: String, errorMessage: String)

  implicit def convertPutRecordBatchResult(result: AWSPutRecordBatchResult): PutRecordBatchResult = {
    PutRecordBatchResult(
      failedPutCount = result.getFailedPutCount,
      records = result.getRequestResponses.immutableSeq.map { record =>
        PutRecordBatchResponseEntry(
          recordId = record.getRecordId,
          errorCode = record.getErrorCode,
          errorMessage = record.getErrorMessage
        )
      }
    )
  }

}
