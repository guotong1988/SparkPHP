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
 * ./bin/SparkSubmit test.php之后就是这里
 * A main class used to launch Php applications. It executes python as a
 * subprocess and then has it connect back to the JVM to access system properties, etc.
 */
object PhpRunner {
  def main(args: Array[String]) {
    val phpFile = args(0)
    val phpFiles = args(1)
    val otherArgs = args.slice(2, args.length)
    val phpExec =
      sys.env.getOrElse("SPARKPHP_DRIVER_PHP", sys.env.getOrElse("SPARKPHP_PYTHON", "php"))

    val formattedPythonFile = formatPath(phpFile)
    val formattedPyFiles = formatPaths(phpFiles)

    val jvmPort = "8080"
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
    // python directories in SPARK_HOME (if set), and any files in the phpFiles argument
    val pathElements = new ArrayBuffer[String]
    pathElements ++= formattedPyFiles
    pathElements += PhpUtils.sparkPhpPath
    pathElements += sys.env.getOrElse("PHPPATH", "")
    val pythonPath = PhpUtils.mergePhpPaths(pathElements: _*)

    // Launch Php process
    val builder = new ProcessBuilder((Seq(phpExec, formattedPythonFile) ++ otherArgs).asJava)
    val env = builder.environment()
    env.put("PHPPATH", pythonPath)
    // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
    env.put("PYTHONUNBUFFERED", "YES") // value is needed to be set to a non-empty string
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

  /**
   * Format the python file path so that it can be added to the PYTHONPATH correctly.
   *
   * Python does not understand URI schemes in paths. Before adding python files to the
   * PYTHONPATH, we need to extract the path from the URI. This is safe to do because we
   * currently only support local python files.
   */
  def formatPath(path: String, testWindows: Boolean = false): String = {
    if (Utils.nonLocalPaths(path, testWindows).nonEmpty) {
      throw new IllegalArgumentException("Launching Python applications through " +
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
      throw new IllegalArgumentException(s"Python file path is malformed: $path")
    }

    // In Windows, the drive should not be prefixed with "/"
    // For instance, python does not understand "/C:/path/to/sheep.py"
    if (Utils.isWindows && formattedPath.matches("/[a-zA-Z]:/.*")) {
      formattedPath = formattedPath.stripPrefix("/")
    }
    formattedPath
  }

  /**
   * Format each python file path in the comma-delimited list of paths, so it can be
   * added to the PYTHONPATH correctly.
   */
  def formatPaths(paths: String, testWindows: Boolean = false): Array[String] = {
    Option(paths).getOrElse("")
      .split(",")
      .filter(_.nonEmpty)
      .map { p => formatPath(p, testWindows) }
  }

}
