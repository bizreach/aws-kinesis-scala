package jp.co.bizreach.kinesis

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import org.apache.spark.rdd.RDD

package object spark {

  implicit class RichRDD[A <: AnyRef](rdd: RDD[A]) {
    /**
     * Save this RDD as records from a producer into an Amazon Kinesis stream.
     *
     * @param streamName Kinesis stream name
     * @param region region name
     * @param credentials credentials provider used to construct a new client.
     *                    By default, [[DefaultAWSCredentialsProviderChain]]
     * @param chunk record size in each PutRecords request. By default, 500
     * @param endpoint service endpoint. By default, None
     */
    def saveToKinesis(streamName: String, region: Regions,
                      credentials: SparkAWSCredentials = DefaultCredentials,
                      chunk: Int = recordsMaxCount,
                      endpoint: Option[String] = None,
                      partitionKeyFunction: Option[A => String] = None): Unit =
      if (!rdd.isEmpty) rdd.sparkContext.runJob(rdd,
        new KinesisRDDWriter(streamName, region, credentials, chunk, endpoint, partitionKeyFunction).write _)
  }

}
