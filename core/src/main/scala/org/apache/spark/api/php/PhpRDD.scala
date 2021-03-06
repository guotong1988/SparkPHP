package org.apache.spark.api.php

import java.io._
import java.net._
import java.util.{Collections, ArrayList => JArrayList, List => JList, Map => JMap}


import org.apache.spark.api.python._

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

class PhpRDD(               var parent: RDD[_],
                            var command: String,
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

  def getJavaRDD:JavaRDD[Array[Byte]]={
    return asJavaRDD
  }

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
                                   command: String,
                                   envVars: JMap[String, String],
                                   phpIncludes: JList[String],
                                   phpExec: String,
                                   phpVer: String,
                                   broadcastVars: JList[Broadcast[PhpBroadcast]],
                                   accumulator: Accumulator[JList[Array[Byte]]],
                                   bufferSize: Int,
                                   reuse_worker: Boolean)
  extends Logging {

//  var file:java.io.File=null
//  var fos:FileWriter=null
//  var osw:BufferedWriter=null
  def compute(
               inputIterator: Iterator[_],
               partitionIndex: Int,
               context: TaskContext): Iterator[Array[Byte]] = {

//    file = new java.io.File("/home/gt/scala_worker3.txt")
//    fos = new java.io.FileWriter(file,true);
//    osw = new BufferedWriter(fos);

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
          var temp = 0
          try {
            temp = stream.readInt()
          }catch{
            case e: Exception  => return null//为了解决两次以上shuffle的问题
          }
//          osw.write("#####"+temp)
//          osw.newLine()
//          osw.flush()
          temp match {
            case length if length > 0 =>
              val obj = new Array[Byte](length)
              stream.readFully(obj)
//              osw.write(">>>>>"+new String(obj))
//              osw.newLine()
//              osw.flush()
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
              // Signals that an exception has been thrown in python
              val exLength = stream.readInt()
              val obj = new Array[Byte](exLength)
              stream.readFully(obj)
              throw new PhpException(new String(obj, UTF_8),
                writerThread.exception.getOrElse(null))
            case SpecialLengths.END_OF_DATA_SECTION =>
              // We've finished the data section of the output, but we can still
              // read some accumulator updates:
              val numAccumulatorUpdates = stream.readInt()

//              osw.write("!!!!!"+numAccumulatorUpdates)
//              osw.newLine()
//              osw.flush()


              (1 to numAccumulatorUpdates).foreach { _ =>
                val updateLen = stream.readInt()
                val update = new Array[Byte](updateLen)
                stream.readFully(update)
                accumulator += Collections.singletonList(update)
              }



              // Check whether the worker is ready to be re-used.
              if (stream.readInt() == SpecialLengths.END_OF_STREAM) {
                if (reuse_worker) {

//                  osw.write("!!!!!!!!!!")
//                  osw.newLine()
//                  osw.flush()

                  env.releasePhpWorker(phpExec, envVars.asScala.toMap, socket2worker)
                  released = true
                }
              }
//              osw.write("!!....")
//              osw.newLine()
//              osw.flush()
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

//        var file:java.io.File=null
//        var fos:FileWriter=null
//        var osw:BufferedWriter=null
//        file = new java.io.File("/home/gt/scala_worker100.txt")
//        fos = new java.io.FileWriter(file,true)
//        osw = new BufferedWriter(fos)

//        osw.write(1)
//        osw.newLine()
//        osw.flush()

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
        val bytes = command.getBytes(UTF_8)
        dataOut.writeInt(bytes.length)
        dataOut.write(bytes)
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






object PhpRDD extends Logging {



  def runJob(
              sc: SparkContext,
              rdd: JavaRDD[Array[Byte]],
              partitions: JArrayList[Int]): Int = {
    type ByteArray = Array[Byte]
    type UnrolledPartition = Array[ByteArray]
    val allPartitions: Array[UnrolledPartition] =
      sc.runJob(rdd, (x: Iterator[ByteArray]) => x.toArray, partitions.asScala)
    val flattenedPartition: UnrolledPartition = Array.concat(allPartitions: _*)
    serveIterator(flattenedPartition.iterator,
      s"serve RDD ${rdd.id} with partitions ${partitions.asScala.mkString(",")}")
  }



  def collectAndServe[T](rdd: RDD[T]): Int = {//被rdd.php的collect方法调用
    serveIterator(rdd.collect().iterator, s"serve RDD ${rdd.id}")
  }
  private val workerBroadcasts = new mutable.WeakHashMap[Socket, mutable.Set[Long]]()
  def getWorkerBroadcasts(worker: Socket): mutable.Set[Long] = {
    synchronized {
      workerBroadcasts.getOrElseUpdate(worker, new mutable.HashSet[Long]())
    }
  }

  /**
   * Return an RDD of values from an RDD of (Long, Array[Byte]), with preservePartitions=true
   *
   * This is useful for PySpark to have the partitioner after partitionBy()
   */
  def valueOfPair(pair: JavaPairRDD[Long, Array[Byte]]): JavaRDD[Array[Byte]] = {
    pair.rdd.mapPartitions(it => it.map(_._2), true)
  }

  def readRDDFromFile(sc: JavaSparkContext, filename: String, parallelism: Int):
  JavaRDD[Array[Byte]] = {
    val file = new DataInputStream(new FileInputStream(filename))
    try {
      val objs = new collection.mutable.ArrayBuffer[Array[Byte]]
      try {
        while (true) {
          val length = file.readInt()
          val obj = new Array[Byte](length)
          file.readFully(obj)
          objs.append(obj)
        }
      } catch {
        case eof: EOFException => {}
      }
      JavaRDD.fromRDD(sc.sc.parallelize(objs, parallelism))
    } finally {
      file.close()
    }
  }


  def serveIterator[T](items: Iterator[T], threadName: String): Int = {
  //  val r = new java.util.Random();
  //  val port = 18082 + r.nextInt(10000)
    val serverSocket = new ServerSocket(0, 1, InetAddress.getByName("localhost"))
  //  val serverSocket = new ServerSocket(port)
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
  //  port
  }

  def writeIteratorToStream[T](iter: Iterator[T], dataOut: DataOutputStream) {

//    var file:java.io.File=null
//    var fos:FileWriter=null
//    var osw:BufferedWriter=null
//    file = new java.io.File("/home/gt/scala_worker.txt")
//    fos = new java.io.FileWriter(file,true)
//    osw = new BufferedWriter(fos)

    def write(obj: Any): Unit = obj match {
      case null =>
        dataOut.writeInt(SpecialLengths.NULL)
      case arr: Array[Byte] =>
//        osw.write("<<<<<"+arr.length)
//        osw.newLine()
//        osw.write("<<<<<" + new String(arr))
//        osw.newLine()
//        osw.flush()
        dataOut.writeInt(arr.length)
        dataOut.write(arr)
      case str: String =>
//        osw.write(">>>>>"+str)
//        osw.newLine()
//        osw.flush()
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

  private def convertRDD[K, V](rdd: RDD[(K, V)],
                               keyConverterClass: String,
                               valueConverterClass: String,
                               defaultConverter: Converter[Any, Any]): RDD[(Any, Any)] = {
    val (kc, vc) = getKeyValueConverters(keyConverterClass, valueConverterClass,
      defaultConverter)
    PhpHadoopUtil.convertRDD(rdd, kc, vc)
  }

  private def getKeyValueConverters(keyConverterClass: String, valueConverterClass: String,
                                    defaultConverter: Converter[Any, Any]): (Converter[Any, Any], Converter[Any, Any]) = {
    val keyConverter = Converter.getInstance(Option(keyConverterClass), defaultConverter)
    val valueConverter = Converter.getInstance(Option(valueConverterClass), defaultConverter)
    (keyConverter, valueConverter)
  }


  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
                                                         sc: JavaSparkContext,
                                                         path: String,
                                                         inputFormatClass: String,
                                                         keyClass: String,
                                                         valueClass: String,
                                                         keyConverterClass: String,
                                                         valueConverterClass: String,
                                                         confAsMap: java.util.HashMap[String, String],
                                                         batchSize: Integer): JavaRDD[Array[Byte]] = {
    var mergedConf :Configuration= null
    if(confAsMap==null){
      mergedConf = sc.hadoopConfiguration()
    }else {
      mergedConf = getMergedConf(confAsMap, sc.hadoopConfiguration())
    }
    val rdd = newAPIHadoopRDDFromClassNames[K, V, F](sc,
        Some(path), inputFormatClass, keyClass, valueClass, mergedConf)
    val confBroadcasted = sc.sc.broadcast(new SerializableConfiguration(mergedConf))
    val converted = convertRDD(rdd, keyConverterClass, valueConverterClass,
      new WritableToJavaConverter(confBroadcasted))
    JavaRDD.fromRDD(SerDeUtil.pairRDDToPhp(converted, batchSize))
  }

  private def newAPIHadoopRDDFromClassNames[K, V, F <: NewInputFormat[K, V]](
                                                                              sc: JavaSparkContext,
                                                                              path: Option[String] = None,
                                                                              inputFormatClass: String,
                                                                              keyClass: String,
                                                                              valueClass: String,
                                                                              conf: Configuration): RDD[(K, V)] = {
    val kc = Utils.classForName(keyClass).asInstanceOf[Class[K]]
    val vc = Utils.classForName(valueClass).asInstanceOf[Class[V]]
    val fc = Utils.classForName(inputFormatClass).asInstanceOf[Class[F]]
    if (path.isDefined) {
      sc.sc.newAPIHadoopFile[K, V, F](path.get, fc, kc, vc, conf)
    } else {
      sc.sc.newAPIHadoopRDD[K, V, F](conf, fc, kc, vc)
    }
  }

  private def getMergedConf(confAsMap: java.util.HashMap[String, String],
                            baseConf: Configuration): Configuration = {
    val conf = PhpHadoopUtil.mapToConf(confAsMap)
    PhpHadoopUtil.mergeConfs(baseConf, conf)
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