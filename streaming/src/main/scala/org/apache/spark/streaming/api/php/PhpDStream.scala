/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.streaming.api.php

import java.io.{BufferedWriter, ObjectInputStream, ObjectOutputStream}
import java.lang.reflect.Proxy
import java.util.{ArrayList => JArrayList, List => JList}

import scala.collection.JavaConverters._
import scala.language.existentials

import py4j.GatewayServer

import org.apache.spark.SparkException
import org.apache.spark.api.java._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Interval, Duration, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.api.java._
import org.apache.spark.util.Utils


/**
 * Interface for Php callback function which is used to transform RDDs
 */
trait PhpTransformFunction {
  def call(time: Long, rdds: JList[_], size:Int): Any

  /**
   * Get the failure, if any, in the last call to `call`.
   *
   * @return the failure message if there was a failure, or `null` if there was no failure.
   */
  def getLastFailure: String
}

/**
 * Interface for Php Serializer to serialize PhpTransformFunction
 */
trait PhpTransformFunctionSerializerInterface {
  def dumps(id: String): Any
  def loads(bytes: Array[Byte]): PhpTransformFunction

  /**
   * Get the failure, if any, in the last call to `dumps` or `loads`.
   *
   * @return the failure message if there was a failure, or `null` if there was no failure.
   */
  def getLastFailure: String
}

/**
 * Wraps a PhpTransformFunction (which is a Php object accessed through Py4J)
 * so that it looks like a Scala function and can be transparently serialized and
 * deserialized by Java.
 */
class TransformFunction(@transient var pfunc: PhpTransformFunction)
  extends function.Function2[JList[JavaRDD[_]], Time, JavaRDD[Array[Byte]]] {

  def apply(rdd: Option[RDD[_]], time: Time): Option[RDD[Array[Byte]]] = {
    val rdds = List(rdd.map(JavaRDD.fromRDD(_)).orNull).asJava
    Option(callPhpTransformFunction(time.milliseconds, rdds)).map(_.rdd)
  }

  def apply(rdd: Option[RDD[_]], rdd2: Option[RDD[_]], time: Time): Option[RDD[Array[Byte]]] = {
    val rdds = List(rdd.map(JavaRDD.fromRDD(_)).orNull, rdd2.map(JavaRDD.fromRDD(_)).orNull).asJava
    Option(callPhpTransformFunction(time.milliseconds, rdds)).map(_.rdd)
  }

  // for function.Function2
  def call(rdds: JList[JavaRDD[_]], time: Time): JavaRDD[Array[Byte]] = {
    callPhpTransformFunction(time.milliseconds, rdds)
  }

  private def callPhpTransformFunction(time: Long, rdds: JList[_]): JavaRDD[Array[Byte]] = {

    val   file = new java.io.File("/home/gt/scala_printer123.txt")
    val   fos = new java.io.FileWriter(file,true)
    val   osw = new BufferedWriter(fos)
    if(rdds!=null) {
      for(i<-0 to rdds.size()-1) {
        osw.write(">>>>>" + rdds.get(i))
        osw.newLine()
      }
    }else{
      osw.write("#####@@@")
      osw.newLine()
    }
    osw.flush()

    var resultRDD: Any = null
    try {
       resultRDD = pfunc.call(time, rdds, rdds.size())
    }catch{
      case e:Exception => {
        osw.write("!!!!!!!!!!!!!"+e.getStackTraceString)
        osw.newLine()
        osw.flush()
        return null
      }
    }

    if(resultRDD!=null) {
      osw.write("#####" + resultRDD)
      osw.newLine()
    }else{
      osw.write("#####$$$")
      osw.newLine()
      osw.flush()
      return null
    }
    osw.flush()

    if(resultRDD.isInstanceOf[String]){
      return null
    }

    val failure = pfunc.getLastFailure
    if (failure != null) {
      throw new SparkException("An exception was raised by Php:\n" + failure)
    }
    resultRDD.asInstanceOf[JavaRDD[Array[Byte]]]
  }

  private def writeObject(out: ObjectOutputStream): Unit = Utils.tryOrIOException {
    try {
      val bytes = PhpTransformFunctionSerializer.serialize(pfunc)
      out.writeInt(bytes.length)
      if (bytes.length > 0) {
        out.write(bytes)
      }
    }catch {
      case e:Exception=>return;
    }
  }

  private def readObject(in: ObjectInputStream): Unit = Utils.tryOrIOException {
    val length = in.readInt()
    if(length>0) {
      val bytes = new Array[Byte](length)
      in.readFully(bytes)
      pfunc = PhpTransformFunctionSerializer.deserialize(bytes)
    }
  }
}

/**
 * Helpers for PhpTransformFunctionSerializer
 *
 * PhpTransformFunctionSerializer is logically a singleton that's happens to be
 * implemented as a Php object.
 */
object PhpTransformFunctionSerializer {

  /**
   * A serializer in Php, used to serialize PhpTransformFunction
    */
  private var serializer: PhpTransformFunctionSerializerInterface = _

  /*
   * Register a serializer from Php, should be called during initialization
   */
  def register(ser: PhpTransformFunctionSerializerInterface): Unit = synchronized {
    serializer = ser
  }

  def serialize(func: PhpTransformFunction): Array[Byte] = synchronized {
    require(serializer != null, "Serializer has not been registered!")
    // get the id of PhpTransformFunction in py4j
 //   val h = Proxy.getInvocationHandler(func.asInstanceOf[Proxy])


    val   file = new java.io.File("/home/gt/scala_printer1.txt")
    val   fos = new java.io.FileWriter(file,true);
    val   osw = new BufferedWriter(fos);
//    osw.write("#####" + h.getClass)
//    osw.newLine()
//    for(e<-h.getClass.getDeclaredFields){
//      osw.write("#####" + e)
//      osw.newLine()
//    }
//    osw.flush()
//
//
//    val f = h.getClass().getDeclaredField("name")
//
//    osw.write("$$$$$" + f)
//    osw.newLine()
//
//    f.setAccessible(true)
//    val id = f.get(h).asInstanceOf[String]
//
//    osw.write("#####" + id)
//    osw.newLine()
//    osw.flush()
    osw.write("<<<<<***")
    osw.newLine()
    osw.flush()
    var results:Any = null;
    try {
   //   results = serializer.dumps("0")
      return new Array[Byte](0)
    }catch{
      case e:Exception => {
        osw.write("!!!!!!!"+e.getStackTraceString)
        osw.newLine()
        osw.flush()
        return new Array[Byte](0)}
    }


    if(results!=null) {
      osw.write(">>>>>" + results)
      osw.newLine()
      osw.flush()
    }else{
      osw.write(">>>>>***")
      osw.newLine()
      osw.flush()
      return new Array[Byte](0)
    }

    val failure = serializer.getLastFailure
    if (failure != null) {
      throw new SparkException("An exception was raised by Php:\n" + failure)
    }

    if(results.isInstanceOf[String]){
      return results.asInstanceOf[String].getBytes
    }
    results.asInstanceOf[Array[Byte]]
  }

  def deserialize(bytes: Array[Byte]): PhpTransformFunction = synchronized {
    require(serializer != null, "Serializer has not been registered!")
    val pfunc = serializer.loads(bytes)
    val failure = serializer.getLastFailure
    if (failure != null) {
      throw new SparkException("An exception was raised by Php:\n" + failure)
    }
    pfunc
  }
}

/**
 * Helper functions, which are called from Php via Py4J.
 */
object PhpDStream {

  /**
   *
   *
   * can not access PhpTransformFunctionSerializer.register() via Py4j
   * Py4JError: PhpTransformFunctionSerializerregister does not exist in the JVM
   */
  def registerSerializer(ser: PhpTransformFunctionSerializerInterface): Unit = {
    PhpTransformFunctionSerializer.register(ser)
  }

  /**
   * Update the port of callback client to `port`
   */
  def updatePhpGatewayPort(gws: GatewayServer, port: Int): Unit = {
    val cl = gws.getCallbackClient
    val f = cl.getClass.getDeclaredField("port")
    f.setAccessible(true)
    f.setInt(cl, port)
  }

  /**
   * helper function for DStream.foreachRDD(),
   * cannot be `foreachRDD`, it will confusing py4j
   */
  def callForeachRDD(jdstream: JavaDStream[Array[Byte]], pfunc: PhpTransformFunction) {
    val func = new TransformFunction((pfunc))
    jdstream.dstream.foreachRDD((rdd, time) => func(Some(rdd), time))
  }

  /**
   * convert list of RDD into queue of RDDs, for ssc.queueStream()
   */
  def toRDDQueue(rdds: JArrayList[JavaRDD[Array[Byte]]]): java.util.Queue[JavaRDD[Array[Byte]]] = {
    val queue = new java.util.LinkedList[JavaRDD[Array[Byte]]]
    rdds.asScala.foreach(queue.add)
    queue
  }
}

/**
 * Base class for PhpDStream with some common methods
 */
abstract class PhpDStream(
    parent: DStream[_],
    pfunc: PhpTransformFunction)
  extends DStream[Array[Byte]] (parent.ssc) {

  val func = new TransformFunction(pfunc)

  override def dependencies: List[DStream[_]] = List(parent)

  override def slideDuration: Duration = parent.slideDuration

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

/**
 * Transformed DStream in Php.
 */
class PhpTransformedDStream (
    parent: DStream[_],
    pfunc: PhpTransformFunction)
  extends PhpDStream(parent, pfunc) {

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val rdd = parent.getOrCompute(validTime)
    if (rdd.isDefined) {
      func(rdd, validTime)
    } else {
      None
    }
  }
}

/**
 * Transformed from two DStreams in Php.
 */
class PhpTransformed2DStream(
    parent: DStream[_],
    parent2: DStream[_],
    pfunc: PhpTransformFunction)
  extends DStream[Array[Byte]] (parent.ssc) {

  val func = new TransformFunction(pfunc)

  override def dependencies: List[DStream[_]] = List(parent, parent2)

  override def slideDuration: Duration = parent.slideDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val empty: RDD[_] = ssc.sparkContext.emptyRDD
    val rdd1 = parent.getOrCompute(validTime).getOrElse(empty)
    val rdd2 = parent2.getOrCompute(validTime).getOrElse(empty)
    func(Some(rdd1), Some(rdd2), validTime)
  }

  val asJavaDStream: JavaDStream[Array[Byte]] = JavaDStream.fromDStream(this)
}

/**
 * similar to StateDStream
 */
class PhpStateDStream(
    parent: DStream[Array[Byte]],
    reduceFunc: PhpTransformFunction)
  extends PhpDStream(parent, reduceFunc) {

  super.persist(StorageLevel.MEMORY_ONLY)
  override val mustCheckpoint = true

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val lastState = getOrCompute(validTime - slideDuration)
    val rdd = parent.getOrCompute(validTime)
    if (rdd.isDefined) {
      func(lastState, rdd, validTime)
    } else {
      lastState
    }
  }
}

/**
 * similar to ReducedWindowedDStream
 */
class PhpReducedWindowedDStream(
    parent: DStream[Array[Byte]],
    preduceFunc: PhpTransformFunction,
    @transient private val pinvReduceFunc: PhpTransformFunction,
    _windowDuration: Duration,
    _slideDuration: Duration)
  extends PhpDStream(parent, preduceFunc) {

  super.persist(StorageLevel.MEMORY_ONLY)

  override val mustCheckpoint: Boolean = true

  val invReduceFunc: TransformFunction = new TransformFunction(pinvReduceFunc)

  def windowDuration: Duration = _windowDuration

  override def slideDuration: Duration = _slideDuration

  override def parentRememberDuration: Duration = rememberDuration + windowDuration

  override def compute(validTime: Time): Option[RDD[Array[Byte]]] = {
    val currentTime = validTime
    val current = new Interval(currentTime - windowDuration, currentTime)
    val previous = current - slideDuration

    //  _____________________________
    // |  previous window   _________|___________________
    // |___________________|       current window        |  --------------> Time
    //                     |_____________________________|
    //
    // |________ _________|          |________ _________|
    //          |                             |
    //          V                             V
    //       old RDDs                     new RDDs
    //
    val previousRDD = getOrCompute(previous.endTime)

    // for small window, reduce once will be better than twice
    if (pinvReduceFunc != null && previousRDD.isDefined
        && windowDuration >= slideDuration * 5) {

      // subtract the values from old RDDs
      val oldRDDs = parent.slice(previous.beginTime + parent.slideDuration, current.beginTime)
      val subtracted = if (oldRDDs.size > 0) {
        invReduceFunc(previousRDD, Some(ssc.sc.union(oldRDDs)), validTime)
      } else {
        previousRDD
      }

      // add the RDDs of the reduced values in "new time steps"
      val newRDDs = parent.slice(previous.endTime + parent.slideDuration, current.endTime)
      if (newRDDs.size > 0) {
        func(subtracted, Some(ssc.sc.union(newRDDs)), validTime)
      } else {
        subtracted
      }
    } else {
      // Get the RDDs of the reduced values in current window
      val currentRDDs = parent.slice(current.beginTime + parent.slideDuration, current.endTime)
      if (currentRDDs.size > 0) {
        func(None, Some(ssc.sc.union(currentRDDs)), validTime)
      } else {
        None
      }
    }
  }
}
