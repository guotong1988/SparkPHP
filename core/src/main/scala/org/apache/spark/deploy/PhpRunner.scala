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

package org.apache.spark.deploy

import java.net.URI
import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.Try

import org.apache.spark.SparkUserAppException
import org.apache.spark.api.php.PhpUtils
import org.apache.spark.util.{RedirectThread, Utils}

/**
 * master用的
 */
object PhpRunner {
  def main(args: Array[String]) {
    val phpFile = args(0)
    val phpFiles = args(1)
    val otherArgs = args.slice(2, args.length)
    val phpExec =
      sys.env.getOrElse("SPARKPHP_DRIVER_PHP", sys.env.getOrElse("SPARKPHP_PHP", "php"))

    val formattedPhpFile = formatPath(phpFile)
    val formattedPhpFiles = formatPaths(phpFiles)

    val jvmPort = "18080"
    val runner = php.java.bridge.JavaBridgeRunner.getInstance(jvmPort);

    val thread = new Thread(new Runnable() {
      override def run(): Unit = Utils.logUncaughtExceptions {
        runner.waitFor()
      }
    })
    thread.setName("php-jvm-init")
    thread.setDaemon(true)
    thread.start()

    //thread.join()

    // Build up a PHPPATH that includes the Spark assembly JAR (where this class is), the
    // php directories in SPARK_HOME (if set), and any files in the phpFiles argument
    val pathElements = new ArrayBuffer[String]
    pathElements ++= formattedPhpFiles
    pathElements += PhpUtils.sparkPhpPath
    pathElements += sys.env.getOrElse("PHPPATH", "")
    val phpPath = PhpUtils.mergePhpPaths(pathElements: _*)

    // Launch Php process
    val builder = new ProcessBuilder((Seq(phpExec, formattedPhpFile) ++ otherArgs).asJava)
    val env = builder.environment()
    env.put("PHPPATH", phpPath)
    env.put("PHPNUNBUFFERED", "YES") // value is needed to be set to a non-empty string
    env.put("SPARKPHP_GATEWAY_PORT", "" + jvmPort)
    builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
    try {
      val process = builder.start()

      new RedirectThread(process.getInputStream, System.out, "redirect output").start()

      val exitCode = process.waitFor()
      if (exitCode != 0) {
        throw new SparkUserAppException(exitCode)
      }
    } finally {
      runner.destroy()
    }
  }


  def formatPath(path: String, testWindows: Boolean = false): String = {
    if (Utils.nonLocalPaths(path, testWindows).nonEmpty) {
      throw new IllegalArgumentException("Launching Php applications through " +
        s"spark-submit is currently only supported for local files: $path")
    }
    // get path when scheme is file.
    val uri = Try(new URI(path)).getOrElse(new File(path).toURI)
    var formattedPath = uri.getScheme match {
      case null => path
      case "file" | "local" => uri.getPath
      case _ => null
    }

    // Guard against malformed paths potentially throwing NPE
    if (formattedPath == null) {
      throw new IllegalArgumentException(s"Php file path is malformed: $path")
    }

    // In Windows, the drive should not be prefixed with "/"
    // For instance, php does not understand "/C:/path/to/sheep.py"
    if (Utils.isWindows && formattedPath.matches("/[a-zA-Z]:/.*")) {
      formattedPath = formattedPath.stripPrefix("/")
    }
    formattedPath
  }

  def formatPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    Option(paths).getOrElse("")
      .split(",")
      .filter(_.nonEmpty)
      .map { p => formatPath(p, testWindows) }
  }

}
