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

private[spark] class PhpRDD(parent: RDD[_],
                            command: Array[Byte],
                            envVars: JMap[String, String],
                            pythonIncludes: JList[String],
                            preservePartitoning: Boolean,
                            pythonExec: String,
                            pythonVer: String,
                            broadcastVars: JList[Broadcast[PhpBroadcast]],
                            accumulator: Accumulator[JList[Array[Byte]]])
  extends RDD[Array[Byte]](parent) {
  override def getPartitions: Array[Partition] = firstParent.partitions
  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    return null;
  }
}

private[spark] object PhpRDD extends Logging {
  def collectAndServe[T](rdd: RDD[T]): Int = {
    serveIterator(rdd.collect().iterator, s"serve RDD ${rdd.id}")
  }

  def serveIterator[T](items: Iterator[T], threadName: String): Int = {
    val serverSocket = new ServerSocket(18081)
    // Close the socket if no connection in 3 seconds
    serverSocket.setSoTimeout(3000)

    new Thread(threadName) {
      setDaemon(true)
      override def run() {
        try {
          val sock = serverSocket.accept()
          val out = new DataOutputStream(new BufferedOutputStream(sock.getOutputStream))
          Utils.tryWithSafeFinally {
            writeIteratorToStream(items, out)
          } {
            out.close()
          }
        } catch {
          case NonFatal(e) =>
            logError(s"Error while sending iterator", e)
        } finally {
          serverSocket.close()
        }
      }
    }.start()

    serverSocket.getLocalPort
  }

  def writeIteratorToStream[T](iter: Iterator[T], dataOut: DataOutputStream) {

    def write(obj: Any): Unit = obj match {
      case null =>
        dataOut.writeInt(SpecialLengths.NULL)
      case arr: Array[Byte] =>
        dataOut.writeInt(arr.length)
        dataOut.write(arr)
      case str: String =>
        writeUTF(str, dataOut)
      case stream: PortableDataStream =>
        write(stream.toArray())
      case (key, value) =>
        write(key)
        write(value)
      case other =>
        throw new SparkException("Unexpected element type " + other.getClass)
    }

    iter.foreach(write)
  }

  def writeUTF(str: String, dataOut: DataOutputStream) {
    val bytes = str.getBytes(UTF_8)
    dataOut.writeInt(bytes.length)
    dataOut.write(bytes)
  }
}


private object SpecialLengths {
  val END_OF_DATA_SECTION = -1
  val PHP_EXCEPTION_THROWN = -2
  val TIMING_DATA = -3
  val END_OF_STREAM = -4
  val NULL = -5
}



/**
 * An Wrapper for Php Broadcast, which is written into disk by Php. It also will
 * write the data into disk after deserialization, then Php can read it from disks.
 */
// scalastyle:off no.finalize
private[spark] class PhpBroadcast(@transient var path: String) extends Serializable
with Logging {

  /**
   * Read data from disks, then copy it to `out`
   */
  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    val in = new FileInputStream(new File(path))
    try {
      Utils.copyStream(in, out)
    } finally {
      in.close()
    }
  }

  /**
   * Write data into disk, using randomly generated name.
   */
  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val dir = new File(Utils.getLocalDir(SparkEnv.get.conf))
    val file = File.createTempFile("broadcast", "", dir)
    path = file.getAbsolutePath
    val out = new FileOutputStream(file)
    Utils.tryWithSafeFinally {
      Utils.copyStream(in, out)
    } {
      out.close()
    }
  }

  /**
   * Delete the file once the object is GCed.
   */
  override def finalize() {
    if (!path.isEmpty) {
      val file = new File(path)
      if (file.exists()) {
        if (!file.delete()) {
          logWarning(s"Error deleting ${file.getPath}")
        }
      }
    }
  }
}