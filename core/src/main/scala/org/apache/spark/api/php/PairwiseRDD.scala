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

    val file = new java.io.File("/home/gt/scala_worker3.txt")
    val fos = new java.io.FileWriter(file);
    val osw = new BufferedWriter(fos);

    val eles = Thread.currentThread().getStackTrace
    for(i <- 0 to eles.length-1) {
      osw.write("<<<<<"+eles(i))
      osw.newLine()
    }
    osw.flush()
    prev.iterator(split, context).map {
      case x => {
        osw.write("#####"+new String(x))
        osw.newLine()
        osw.flush()
        val temp = new String(x)
        val temp2 = temp.split(">>>")
        var split = temp2(0)
        var key = ""
        var value = ""
        if(temp2.length>1){
         key = temp2(1)
         value = temp2(2)
        }
        if(split.equals("")){
          split="0";
        }
        (java.lang.Long.valueOf(split).longValue(),(key+">>>"+value).getBytes)

      }
   /*   case Seq(a, b) => {

        (this.deserializeLongValue(a), b)}

      case x:List[Byte] => {

        osw.write("@@@@@"+x.length)
        osw.newLine()
        osw.flush()

        try{
          osw.write("^^^^^"+x.getClass)
          osw.newLine()
          osw.flush()

        }catch{
          case ex: Exception =>{}
        }

        (1L,"abcde".getBytes)
      }*/
      case x => throw new SparkException("PairwiseRDD: unexpected value: " + x)
    }
  }

  def deserializeLongValue(bytes: Array[Byte]) : Long = {
    val temp = new Array[Byte](8);
    if(bytes.length==8) {
      temp(7) = bytes(7);
      temp(6) = bytes(6);
      temp(5) = bytes(5);
      temp(4) = bytes(4);
      temp(3) = bytes(3);
      temp(2) = bytes(2);
      temp(1) = bytes(1);
      temp(0) = bytes(0);
    }
    if(bytes.length==7) {
      temp(7) = 0;
      temp(6) = bytes(6);
      temp(5) = bytes(5);
      temp(4) = bytes(4);
      temp(3) = bytes(3);
      temp(2) = bytes(2);
      temp(1) = bytes(1);
      temp(0) = bytes(0);
    }
    if(bytes.length==6) {
      temp(7) = 0;
      temp(6) = 0;
      temp(5) = bytes(5);
      temp(4) = bytes(4);
      temp(3) = bytes(3);
      temp(2) = bytes(2);
      temp(1) = bytes(1);
      temp(0) = bytes(0);
    }
    if(bytes.length==5) {
      temp(7) = 0;
      temp(6) = 0;
      temp(5) = 0;
      temp(4) = bytes(4);
      temp(3) = bytes(3);
      temp(2) = bytes(2);
      temp(1) = bytes(1);
      temp(0) = bytes(0);
    }
    if(bytes.length==4) {
      temp(7) = 0;
      temp(6) = 0;
      temp(5) = 0;
      temp(4) = 0;
      temp(3) = bytes(3);
      temp(2) = bytes(2);
      temp(1) = bytes(1);
      temp(0) = bytes(0);
    }
    if(bytes.length==3) {
      temp(7) = 0;
      temp(6) = 0;
      temp(5) = 0;
      temp(4) = 0;
      temp(3) = 0;
      temp(2) = bytes(2);
      temp(1) = bytes(1);
      temp(0) = bytes(0);
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