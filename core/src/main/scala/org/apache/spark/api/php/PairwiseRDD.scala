package org.apache.spark.api.php


import org.apache.spark._
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

/**
 * Form an RDD[(Array[Byte], Array[Byte])] from key-value pairs returned from Php.
 * This is used by SparkPhp's shuffle operations.
 */
class PairwiseRDD(prev: RDD[Array[Byte]]) extends RDD[(Long, Array[Byte])](prev) {
  override def getPartitions: Array[Partition] = prev.partitions
  override val partitioner: Option[Partitioner] = prev.partitioner
  override def compute(split: Partition, context: TaskContext): Iterator[(Long, Array[Byte])] =
    prev.iterator(split, context).grouped(2).map {
      case Seq(a, b) => (Utils.deserializeLongValue(a), b)
      case x => throw new SparkException("PairwiseRDD: unexpected value: " + x)
    }
  val asJavaPairRDD : JavaPairRDD[Long, Array[Byte]] = JavaPairRDD.fromRDD(this)
}