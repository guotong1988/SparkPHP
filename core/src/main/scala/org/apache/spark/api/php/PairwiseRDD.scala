package org.apache.spark.api.php


import java.io.BufferedWriter

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
  override def compute(split: Partition, context: TaskContext): Iterator[(Long, Array[Byte])] = {
  //  var file = new java.io.File("/home/gt/scala_worker11.txt")
  //  var fos = new java.io.FileWriter(file);
  //  var osw = new BufferedWriter(fos);
    prev.iterator(split, context).grouped(2).map {
      case Seq(a, b) => {

  //      osw.write("-----"+a)
  //      osw.newLine()
  //      osw.flush()

   //     osw.write("+++++"+b)
   //     osw.newLine()
    //    osw.flush()

    //    osw.write("====="+Utils.deserializeLongValue(a))
    //    osw.newLine()
    //    osw.flush()

        (Utils.deserializeLongValue(a), b)
      }
      case x => throw new SparkException("PairwiseRDD: unexpected value: " + x)
    }
  }

  def deserializeLongValue(bytes: Array[Byte]) : Long = {
    val temp = new Array[Byte](8);
    if(bytes.length==8) {
      temp(7) = bytes(0);
      temp(6) = bytes(1);
      temp(5) = bytes(2);
      temp(4) = bytes(3);
      temp(3) = bytes(4);
      temp(2) = bytes(5);
      temp(1) = bytes(6);
      temp(0) = bytes(7);
    }
    if(bytes.length==7) {
      temp(7) = bytes(0);
      temp(6) = bytes(1);
      temp(5) = bytes(2);
      temp(4) = bytes(3);
      temp(3) = bytes(4);
      temp(2) = bytes(5);
      temp(1) = bytes(6);
      temp(0) = 0;
    }
    if(bytes.length==6) {
      temp(7) = bytes(0);
      temp(6) = bytes(1);
      temp(5) = bytes(2);
      temp(4) = bytes(3);
      temp(3) = bytes(4);
      temp(2) = bytes(5);
      temp(1) = 0;
      temp(0) = 0;
    }
    if(bytes.length==5) {
      temp(7) = bytes(0);
      temp(6) = bytes(1);
      temp(5) = bytes(2);
      temp(4) = bytes(3);
      temp(3) = bytes(4);
      temp(2) = 0;
      temp(1) = 0;
      temp(0) = 0;
    }
    if(bytes.length==4) {
      temp(7) = bytes(0);
      temp(6) = bytes(1);
      temp(5) = bytes(2);
      temp(4) = bytes(3);
      temp(3) = 0;
      temp(2) = 0;
      temp(1) = 0;
      temp(0) = 0;
    }
    if(bytes.length==3) {
      temp(7) = bytes(0);
      temp(6) = bytes(1);
      temp(5) = bytes(2);
      temp(4) = 0;
      temp(3) = 0;
      temp(2) = 0;
      temp(1) = 0;
      temp(0) = 0;
    }
    if(bytes.length==2) {
      temp(7) = bytes(0);
      temp(6) = bytes(1)
      temp(5) = 0;
      temp(4) = 0;
      temp(3) = 0;
      temp(2) = 0;
      temp(1) = 0;
      temp(0) = 0;
    }
    if(bytes.length==1) {
      temp(7) =  bytes(0);
      temp(6) = 0;
      temp(5) = 0;
      temp(4) = 0;
      temp(3) = 0;
      temp(2) = 0;
      temp(1) = 0;
      temp(0) = 0;
    }
    if(bytes.length==0) {
      temp(7) = 0;
      temp(6) = 0;
      temp(5) = 0;
      temp(4) = 0;
      temp(3) = 0;
      temp(2) = 0;
      temp(1) = 0;
      temp(0) = 0;
    }

    var result = temp(7) & 0xFFL
    result = result + ((temp(6) & 0xFFL) << 8)
    result = result + ((temp(5) & 0xFFL) << 16)
    result = result + ((temp(4) & 0xFFL) << 24)
    result = result + ((temp(3) & 0xFFL) << 32)
    result = result + ((temp(2) & 0xFFL) << 40)
    result = result + ((temp(1) & 0xFFL) << 48)
    result + ((temp(0) & 0xFFL) << 56)
  }

  val asJavaPairRDD : JavaPairRDD[Long, Array[Byte]] = JavaPairRDD.fromRDD(this)
}