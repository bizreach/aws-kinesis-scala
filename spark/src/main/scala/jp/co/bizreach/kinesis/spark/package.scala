package jp.co.bizreach.kinesis

import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain}
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration
import com.amazonaws.regions.Regions
import org.apache.spark.rdd.RDD

package object spark {

  implicit class RichRDD[A <: AnyRef](rdd: RDD[A]) {
    /**
     * Save this RDD as records from a producer into an Amazon Kinesis stream.
     *
     * Note: The AWS credentials will be discovered using the InstanceProfileCredentialsProvider
     * on the workers.
     *
     * @param streamName Kinesis stream name
     * @param region region name
     * @param credentials a credentials provider to use to constructs a new client.
     *                    By default, [[DefaultAWSCredentialsProviderChain]]
     * @param chunk record size in each PutRecords request. By default, 500
     */
    def saveToKinesis(streamName: String, region: Regions,
                      credentials: Class[_ <: AWSCredentialsProvider],
                      chunk: Int,
                      endpoint: Option[String]): Unit = {
      val creds = Option(credentials).getOrElse(classOf[DefaultAWSCredentialsProviderChain]).getConstructor().newInstance()
      val chunkSize = Option(chunk).getOrElse(recordsMaxCount)
      saveToKinesis(streamName, region, credentials, chunkSize, endpoint)
    }


    /**
     * Save this RDD as records from a producer into an Amazon Kinesis stream.
     *
     * @param streamName Kinesis stream name
     * @param region region name
     * @param credentials a credentials provider to use to constructs a new client.
     *                    By default, [[DefaultAWSCredentialsProviderChain]]
     * @param chunk record size in each PutRecords request. By default, 500
     */
    def saveToKinesis(streamName: String, region: Regions,
                      credentials: AWSCredentialsProvider,
                      chunk: Int,
                      endpoint: Option[String]): Unit = {
      val chunkSize = Option(chunk).getOrElse(recordsMaxCount)
      if (!rdd.isEmpty) rdd.sparkContext.runJob(rdd,
        new KinesisRDDWriter(streamName, region, credentials, chunkSize, endpoint).write)
    }
  }

}
