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

package org.apache.spark.sql.execution

import java.io.{BufferedWriter, OutputStream}
import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._

import net.razorvine.pickle._

import org.apache.spark.{Logging => SparkLogging, TaskContext, Accumulator}
import org.apache.spark.api.php.{PhpRDD, SerDeUtil}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{MapData, GenericArrayData, ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A serialized version of a Python lambda function.  Suitable for use in a [[PhpRDD]].
 */

object EvaluatePhp {

  def takeAndServe(df: DataFrame, n: Int): Int = {
    registerPicklers()
    df.withNewExecutionId {
      val iter = new SerDeUtil.AutoBatchedPickler(
        df.queryExecution.executedPlan.executeTake(n).iterator.map { row =>
          EvaluatePhp.toJava(row, df.schema)
        })
      PhpRDD.serveIterator(iter, s"serve-DataFrame")
    }
  }

  /**
   * Helper for converting from Catalyst type to java type suitable for Pyrolite.
   */
  def toJava(obj: Any, dataType: DataType): Any = (obj, dataType) match {
    case (null, _) => null

    case (row: InternalRow, struct: StructType) =>
      val values = new Array[Any](row.numFields)
      var i = 0
      while (i < row.numFields) {
        values(i) = toJava(row.get(i, struct.fields(i).dataType), struct.fields(i).dataType)


        val   file = new java.io.File("/home/gt/scala_worker7.txt")
        val   fos = new java.io.FileWriter(file,true);
        val   osw = new BufferedWriter(fos);
        osw.write("#####" + values(i))
        osw.newLine()
        osw.flush()

        i += 1
      }
      values
     // new GenericInternalRowWithSchema(values, struct)

    case (a: ArrayData, array: ArrayType) =>
      val values = new java.util.ArrayList[Any](a.numElements())
      a.foreach(array.elementType, (_, e) => {
        values.add(toJava(e, array.elementType))
      })
      values

    case (map: MapData, mt: MapType) =>
      val jmap = new java.util.HashMap[Any, Any](map.numElements())
      map.foreach(mt.keyType, mt.valueType, (k, v) => {
        jmap.put(toJava(k, mt.keyType), toJava(v, mt.valueType))
      })
      jmap

    case (ud, udt: UserDefinedType[_]) => toJava(ud, udt.sqlType)

    case (d: Decimal, _) => d.toJavaBigDecimal

    case (s: UTF8String, StringType) => s.toString

    case (other, _) => other
  }

  /**
   * Converts `obj` to the type specified by the data type, or returns null if the type of obj is
   * unexpected. Because Python doesn't enforce the type.
   */
  def fromJava(obj: Any, dataType: DataType): Any = (obj, dataType) match {
    case (null, _) => null

    case (c: Boolean, BooleanType) => c

    case (c: Int, ByteType) => c.toByte
    case (c: Long, ByteType) => c.toByte

    case (c: Int, ShortType) => c.toShort
    case (c: Long, ShortType) => c.toShort

    case (c: Int, IntegerType) => c
    case (c: Long, IntegerType) => c.toInt

    case (c: Int, LongType) => c.toLong
    case (c: Long, LongType) => c

    case (c: Double, FloatType) => c.toFloat

    case (c: Double, DoubleType) => c

    case (c: java.math.BigDecimal, dt: DecimalType) => Decimal(c, dt.precision, dt.scale)

    case (c: Int, DateType) => c

    case (c: Long, TimestampType) => c

    case (c: String, StringType) => {
      val   file = new java.io.File("/home/gt/scala_worker45.txt")
      val   fos = new java.io.FileWriter(file,true);
      val   osw = new BufferedWriter(fos);
      osw.write("#####" + c)
      osw.newLine()
      osw.flush()

      val temp = UTF8String.fromString(c)

      osw.write("#####" + temp)
      osw.newLine()
      osw.flush()

      temp
    }
    case (c, StringType) => {
      val   file = new java.io.File("/home/gt/scala_worker46.txt")
      val   fos = new java.io.FileWriter(file,true);
      val   osw = new BufferedWriter(fos);
      osw.write("#####" + c)
      osw.newLine()
      osw.flush()

      // If we get here, c is not a string. Call toString on it.
      val temp = UTF8String.fromString(c.toString)

      osw.write("#####" + temp)
      osw.newLine()
      osw.flush()

      temp
    }
    case (c: String, BinaryType) => c.getBytes("utf-8")
    case (c, BinaryType) if c.getClass.isArray && c.getClass.getComponentType.getName == "byte" => c

    case (c: java.util.List[_], ArrayType(elementType, _)) =>
      new GenericArrayData(c.asScala.map { e => fromJava(e, elementType)}.toArray)

    case (c, ArrayType(elementType, _)) if c.getClass.isArray =>
      new GenericArrayData(c.asInstanceOf[Array[_]].map(e => fromJava(e, elementType)))

    case (c: java.util.Map[_, _], MapType(keyType, valueType, _)) =>
      val keyValues = c.asScala.toSeq
      val keys = keyValues.map(kv => fromJava(kv._1, keyType)).toArray
      val values = keyValues.map(kv => fromJava(kv._2, valueType)).toArray
      ArrayBasedMapData(keys, values)

    case (c, StructType(fields)) if c.getClass.isArray => {

      val   file = new java.io.File("/home/gt/scala_worker44.txt")
      val   fos = new java.io.FileWriter(file,true);
      val   osw = new BufferedWriter(fos);
      osw.write("#####" + c)
      osw.newLine()
      osw.flush()

      new GenericInternalRow(c.asInstanceOf[Array[_]].zip(fields).map {
        case (e, f) => fromJava(e, f.dataType)
      })
    }
    case (_, udt: UserDefinedType[_]) => fromJava(obj, udt.sqlType)

    // all other unexpected type should be null, or we will have runtime exception
    // TODO(davies): we could improve this by try to cast the object to expected type
    case (c, _) => {

      val   file = new java.io.File("/home/gt/scala_worker43.txt")
      val   fos = new java.io.FileWriter(file,true);
      val   osw = new BufferedWriter(fos);
      osw.write("#####" + c)
      osw.newLine()
      osw.flush()

      null
    }
  }


  private val module = "pyspark.sql.types"

  /**
   * Pickler for StructType
   */
  private class StructTypePickler extends IObjectPickler {

    private val cls = classOf[StructType]

    def register(): Unit = {
      Pickler.registerCustomPickler(cls, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      out.write(Opcodes.GLOBAL)
      out.write((module + "\n" + "_parse_datatype_json_string" + "\n").getBytes("utf-8"))
      val schema = obj.asInstanceOf[StructType]
      pickler.save(schema.json)


      val   file = new java.io.File("/home/gt/scala_worker18.txt")
      val   fos = new java.io.FileWriter(file,true);
      val   osw = new BufferedWriter(fos);
      osw.write("#####" + schema.json)
      osw.newLine()
      osw.flush()

      out.write(Opcodes.TUPLE1)
      out.write(Opcodes.REDUCE)
      throw new Exception()
    }
  }

  /**
   * Pickler for InternalRow
   */
  private class RowPickler extends IObjectPickler {

    private val cls = classOf[GenericInternalRowWithSchema]

    // register this to Pickler and Unpickler
    def register(): Unit = {
      Pickler.registerCustomPickler(this.getClass, this)
      Pickler.registerCustomPickler(cls, this)
    }

    def pickle(obj: Object, out: OutputStream, pickler: Pickler): Unit = {
      if (obj == this) {
        out.write(Opcodes.GLOBAL)
        out.write((module + "\n" + "_create_row_inbound_converter" + "\n").getBytes("utf-8"))
      } else {
        // it will be memorized by Pickler to save some bytes
        pickler.save(this)
        val row = obj.asInstanceOf[GenericInternalRowWithSchema]
        // schema should always be same object for memoization
        pickler.save(row.schema)
        out.write(Opcodes.TUPLE1)
        out.write(Opcodes.REDUCE)

        out.write(Opcodes.MARK)
        var i = 0
        while (i < row.values.size) {
          pickler.save(row.values(i))



          val   file = new java.io.File("/home/gt/scala_worker17.txt")
          val   fos = new java.io.FileWriter(file,true);
          val   osw = new BufferedWriter(fos);
          osw.write("#####" + row.values(i))
          osw.newLine()
          osw.flush()

          i += 1

        }
        out.write(Opcodes.TUPLE)
        out.write(Opcodes.REDUCE)
        throw new Exception()
      }
    }
  }

  private[this] var registered = false
  /**
   * This should be called before trying to serialize any above classes un cluster mode,
   * this should be put in the closure
   */
  def registerPicklers(): Unit = {
    synchronized {
      if (!registered) {
        SerDeUtil.initialize()
        new StructTypePickler().register()
        new RowPickler().register()
        registered = true
      }
    }
  }

  /**
   * Convert an RDD of Java objects to an RDD of serialized Php objects, that is usable by
   * PySpark.
   */
  def javaToPhp(rdd: RDD[Any]): RDD[Array[Byte]] = {
    rdd.mapPartitions { iter =>
      registerPicklers()  // let it called in executor
      new SerDeUtil.AutoBatchedPickler(iter)
    }
  }
}
