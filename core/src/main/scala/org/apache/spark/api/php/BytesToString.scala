package org.apache.spark.api.php
import com.google.common.base.Charsets.UTF_8
class BytesToString extends org.apache.spark.api.java.function.Function[Array[Byte], String] {
override def call(arr: Array[Byte]) : String = new String(arr, UTF_8)
}