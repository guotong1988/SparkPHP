package org.apache.spark.api.php
import java.io._
import java.net._
import java.util.{Collections, ArrayList => JArrayList, List => JList, Map => JMap}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.existentials
import scala.util.control.NonFatal

import com.google.common.base.Charsets.UTF_8
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.{InputFormat, JobConf, OutputFormat}
import org.apache.hadoop.mapreduce.{InputFormat => NewInputFormat, OutputFormat => NewOutputFormat}

import org.apache.spark._
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{SerializableConfiguration, Utils}

class PhpAccumulatorParam(@transient private val serverHost: String, serverPort: Int)
  extends AccumulatorParam[JList[Array[Byte]]] {


  Utils.checkHost(serverHost, "Expected hostname")

  val bufferSize = SparkEnv.get.conf.getInt("spark.buffer.size", 65536)

  /**
   * We try to reuse a single Socket to transfer accumulator updates, as they are all added
   * by the DAGScheduler's single-threaded RpcEndpoint anyway.
   */
  @transient var socket: Socket = _

  def openSocket(): Socket = synchronized {
    if (socket == null || socket.isClosed) {
      socket = new Socket(serverHost, serverPort)
    }
    socket
  }

  override def zero(value: JList[Array[Byte]]): JList[Array[Byte]] = new JArrayList

  override def addInPlace(val1: JList[Array[Byte]], val2: JList[Array[Byte]])
  : JList[Array[Byte]] = synchronized {
    if (serverHost == null) {
      // This happens on the worker node, where we just want to remember all the updates
      val1.addAll(val2)
      val1
    } else {
      // This happens on the master, where we pass the updates to Python through a socket
      val socket = openSocket()
      val in =  new DataInputStream(new BufferedInputStream(socket.getInputStream,bufferSize))
      val out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream, bufferSize))
      out.writeInt(val2.size)
      for (array <- val2.asScala) {
        out.writeInt(array.length)
        out.write(array)
      }
      out.flush()
      // Wait for a byte from the Python side as an acknowledgement
  /*    val byteRead = in.readInt()

      val  file = new java.io.File("/home/gt/scala_worker2.txt")
      val  fos = new java.io.FileWriter(file,true);
      val  osw = new BufferedWriter(fos);

      osw.write(byteRead+"@@@@@")
      osw.newLine()
      osw.flush()


      if (byteRead == -1) {
        throw new SparkException("EOF reached before Python server acknowledged")
      }
  */
      null
    }
  }
}