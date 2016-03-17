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

private[spark] class PhpRDD(var parent: RDD[_],
                            var command: Array[Byte],
                            var envVars: JMap[String, String],
                            var phpIncludes: JList[String],
                            var preservePartitoning: Boolean,
                            var phpExec: String,
                            var phpVer: String,
                            var broadcastVars: JList[Broadcast[PhpBroadcast]],
                            var accumulator: Accumulator[JList[Array[Byte]]]
                            )
  extends RDD[Array[Byte]](parent) {

  val bufferSize = conf.getInt("spark.buffer.size", 65536)
  val reuse_worker = conf.getBoolean("spark.php.worker.reuse", true)

  val asJavaRDD: JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)

  override val partitioner: Option[Partitioner] = {
    if (preservePartitoning) firstParent.partitioner else None
  }

  override def getPartitions: Array[Partition] = firstParent.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[Array[Byte]] = {
    val runner = new PhpRunner4worker(
      command, envVars, phpIncludes, phpExec, phpVer, broadcastVars, accumulator,
      bufferSize, reuse_worker)
    runner.compute(firstParent.iterator(split, context), split.index, context)
  }
}



private[spark] class PhpRunner4worker(
                                   command: Array[Byte],
                                   envVars: JMap[String, String],
                                   phpIncludes: JList[String],
                                   phpExec: String,
                                   phpVer: String,
                                   broadcastVars: JList[Broadcast[PhpBroadcast]],
                                   accumulator: Accumulator[JList[Array[Byte]]],
                                   bufferSize: Int,
                                   reuse_worker: Boolean)
  extends Logging {

  def compute(
               inputIterator: Iterator[_],
               partitionIndex: Int,
               context: TaskContext): Iterator[Array[Byte]] = {

    val startTime = System.currentTimeMillis
    val env = SparkEnv.get
    val localdir = env.blockManager.diskBlockManager.localDirs.map(f => f.getPath()).mkString(",")
    envVars.put("SPARK_LOCAL_DIRS", localdir) // it's also used in monitor thread
    if (reuse_worker) {
      envVars.put("SPARK_REUSE_WORKER", "1")
    }
    val socket2worker: Socket = env.createPhpWorker(phpExec, envVars.asScala.toMap)
    @volatile var released = false
    val writerThread = new WriterThread(env, socket2worker, inputIterator, partitionIndex, context)

    context.addTaskCompletionListener { context =>
      writerThread.shutdownOnTaskCompletion()
      if (!reuse_worker || !released) {
        try {
          socket2worker.close()
        } catch {
          case e: Exception =>
            logWarning("Failed to close worker socket", e)
        }
      }
    }
    writerThread.start()
    new MonitorThread(env, socket2worker, context).start()

    // Return an iterator that read lines from the process's stdout
    val stream = new DataInputStream(new BufferedInputStream(socket2worker.getInputStream, bufferSize))
    val stdoutIterator = new Iterator[Array[Byte]] {
      override def next(): Array[Byte] = {
        val obj = _nextObj
        if (hasNext) {
          _nextObj = read()
        }
        obj
      }

      private def read(): Array[Byte] = {
        if (writerThread.exception.isDefined) {
          throw writerThread.exception.get
        }
        try {
          stream.readInt() match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              stream.readFully(obj)
              obj
            case 0 => Array.empty[Byte]
            case SpecialLengths.TIMING_DATA =>
              // Timing data from worker
              val bootTime = stream.readLong()
              val initTime = stream.readLong()
              val finishTime = stream.readLong()
              val boot = bootTime - startTime
              val init = initTime - bootTime
              val finish = finishTime - initTime
              val total = finishTime - startTime
              logInfo("Times: total = %s, boot = %s, init = %s, finish = %s".format(total, boot,
                init, finish))
              val memoryBytesSpilled = stream.readLong()
              val diskBytesSpilled = stream.readLong()
              context.taskMetrics.incMemoryBytesSpilled(memoryBytesSpilled)
              context.taskMetrics.incDiskBytesSpilled(diskBytesSpilled)
              read()
            case SpecialLengths.PHP_EXCEPTION_THROWN =>
              // Signals that an exception has been thrown in php
              val exLength = stream.readInt()
              val obj = new Array[Byte](exLength)
              stream.readFully(obj)
              throw new PhpException(new String(obj, UTF_8),
                writerThread.exception.getOrElse(null))
            case SpecialLengths.END_OF_DATA_SECTION =>
              // We've finished the data section of the output, but we can still
              // read some accumulator updates:
              val numAccumulatorUpdates = stream.readInt()
              (1 to numAccumulatorUpdates).foreach { _ =>
                val updateLen = stream.readInt()
                val update = new Array[Byte](updateLen)
                stream.readFully(update)
                accumulator += Collections.singletonList(update)
              }
              // Check whether the worker is ready to be re-used.
              if (stream.readInt() == SpecialLengths.END_OF_STREAM) {
                if (reuse_worker) {
                  env.releasePhpWorker(phpExec, envVars.asScala.toMap, socket2worker)
                  released = true
                }
              }
              null
          }
        } catch {

          case e: Exception if context.isInterrupted =>
            logDebug("Exception thrown after task interruption", e)
            throw new TaskKilledException

          case e: Exception if env.isStopped =>
            logDebug("Exception thrown after context is stopped", e)
            null  // exit silently

          case e: Exception if writerThread.exception.isDefined =>
            logError("Php worker exited unexpectedly (crashed)", e)
            logError("This may have been caused by a prior exception:", writerThread.exception.get)
            throw writerThread.exception.get

          case eof: EOFException =>
            throw new SparkException("Php worker exited unexpectedly (crashed)", eof)
        }
      }

      var _nextObj = read()

      override def hasNext: Boolean = _nextObj != null
    }
    new InterruptibleIterator(context, stdoutIterator)


  }

  class WriterThread(
                      env: SparkEnv,
                      socket2worker: Socket,
                      inputIterator: Iterator[_],
                      partitionIndex: Int,
                      context: TaskContext)
                      extends Thread(s"stdout writer for $phpExec") {
    @volatile private var _exception: Exception = null

    setDaemon(true)

    /** Contains the exception thrown while writing the parent iterator to the Php process. */
    def exception: Option[Exception] = Option(_exception)

    /** Terminates the writer thread, ignoring any exceptions that may occur due to cleanup. */
    def shutdownOnTaskCompletion() {
      assert(context.isCompleted)
      this.interrupt()
    }

    override def run(): Unit = Utils.logUncaughtExceptions {
      try {
        TaskContext.setTaskContext(context)
        val stream = new BufferedOutputStream(socket2worker.getOutputStream, bufferSize)
        val dataOut = new DataOutputStream(stream)
        // Partition index
        dataOut.writeInt(partitionIndex)
        // Php version of driver
        PhpRDD.writeUTF(phpVer, dataOut)
        // sparkFilesDir
        PhpRDD.writeUTF(SparkFiles.getRootDirectory(), dataOut)
        // Php includes (*.zip and *.egg files)
        dataOut.writeInt(phpIncludes.size())
        for (include <- phpIncludes.asScala) {
          PhpRDD.writeUTF(include, dataOut)
        }
        // Broadcast variables
        val oldBids = PhpRDD.getWorkerBroadcasts(socket2worker)
        val newBids = broadcastVars.asScala.map(_.id).toSet
        // number of different broadcasts
        val toRemove = oldBids.diff(newBids)
        val cnt = toRemove.size + newBids.diff(oldBids).size
        dataOut.writeInt(cnt)
        for (bid <- toRemove) {
          // remove the broadcast from worker
          dataOut.writeLong(- bid - 1)  // bid >= 0
          oldBids.remove(bid)
        }
        for (broadcast <- broadcastVars.asScala) {
          if (!oldBids.contains(broadcast.id)) {
            // send new broadcast
            dataOut.writeLong(broadcast.id)
            PhpRDD.writeUTF(broadcast.value.path, dataOut)
            oldBids.add(broadcast.id)
          }
        }
        dataOut.flush()
        // Serialized command:
        dataOut.writeInt(command.length)
        dataOut.write(command)
        // Data values
        PhpRDD.writeIteratorToStream(inputIterator, dataOut)
        dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION)
        dataOut.writeInt(SpecialLengths.END_OF_STREAM)
        dataOut.flush()
      } catch {
        case e: Exception if context.isCompleted || context.isInterrupted =>
          logDebug("Exception thrown after task completion (likely due to cleanup)", e)
          if (!socket2worker.isClosed) {
            Utils.tryLog(socket2worker.shutdownOutput())
          }

        case e: Exception =>
          // We must avoid throwing exceptions here, because the thread uncaught exception handler
          // will kill the whole executor (see org.apache.spark.executor.Executor).
          _exception = e
          if (!socket2worker.isClosed) {
            Utils.tryLog(socket2worker.shutdownOutput())
          }
      }
    }
  }

  class MonitorThread(env: SparkEnv, worker: Socket, context: TaskContext)
    extends Thread(s"Worker Monitor for $phpExec") {

    setDaemon(true)

    override def run() {
      // Kill the worker if it is interrupted, checking until task completion.
      // TODO: This has a race condition if interruption occurs, as completed may still become true.
      while (!context.isInterrupted && !context.isCompleted) {
        Thread.sleep(2000)
      }
      if (!context.isCompleted) {
        try {
          logWarning("Incomplete task interrupted: Attempting to kill Php Worker")
          env.destroyPhpWorker(phpExec, envVars.asScala.toMap, worker)
        } catch {
          case e: Exception =>
            logError("Exception when trying to kill worker", e)
        }
      }
    }
  }

}



private class PhpException(msg: String, cause: Exception) extends RuntimeException(msg, cause)






private[spark] object PhpRDD extends Logging {
  def collectAndServe[T](rdd: RDD[T]): Int = {//被rdd.php的collect方法调用
    serveIterator(rdd.collect().iterator, s"serve RDD ${rdd.id}")
  }
  private val workerBroadcasts = new mutable.WeakHashMap[Socket, mutable.Set[Long]]()
  def getWorkerBroadcasts(worker: Socket): mutable.Set[Long] = {
    synchronized {
      workerBroadcasts.getOrElseUpdate(worker, new mutable.HashSet[Long]())
    }
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