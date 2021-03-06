package org.apache.spark.api.php
import org.apache.spark.Logging

import java.io._
import java.net.{InetAddress, ServerSocket, Socket, SocketException}
import java.util.Arrays

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.spark._
import org.apache.spark.util.{RedirectThread, Utils}


/**
 * worker端用的
 */
private[spark] class PhpWorkerFactory(phpExec: String, envVars: Map[String, String])
  extends Logging {

  import PhpWorkerFactory._

  // Because forking processes from Java is expensive, we prefer to launch a single Php daemon
  // (php/src/daemon.py) and tell it to fork new workers for our tasks. This daemon currently
  // only works on UNIX-based systems now because it uses signals for child management, so we can
  // also fall back to launching workers (php/src/worker.py) directly.
  val useDaemon = !System.getProperty("os.name").startsWith("Windows")
  var phpSrcPath : String = null;
  var daemon: Process = null
  val daemonHost = InetAddress.getByAddress(Array(127, 0, 0, 1))
  var daemonPort: Int = 0
  val daemonWorkers = new mutable.WeakHashMap[Socket, Int]()
  val idleWorkers = new mutable.Queue[Socket]()
  var lastActivity = 0L
  new MonitorThread().start()

  var simpleWorkers = new mutable.WeakHashMap[Socket, Process]()

  val phpPath = PhpUtils.mergePhpPaths(
    PhpUtils.sparkPhpPath,
    envVars.getOrElse("PHPPATH", ""),//Yarn模式下是在Client.scala设置的
    sys.env.getOrElse("PHPPATH", ""))

    if(sys.env.getOrElse("PHPPATH", "").contains("SparkPHP.zip")){
      ZipUtils.extract(new File(sys.env.getOrElse("PHPPATH", "")),new File(sys.env.getOrElse("PHPPATH", "").replace("SparkPHP.zip","")))
      phpSrcPath=sys.env.getOrElse("PHPPATH", "").replace("SparkPHP.zip","")
    }else if(envVars.getOrElse("PHPPATH", "").contains("SparkPHP.zip")){
      ZipUtils.extract(new File(envVars.getOrElse("PHPPATH", "")),new File(envVars.getOrElse("PHPPATH", "").replace("SparkPHP.zip","")))
      phpSrcPath=sys.env.getOrElse("PHPPATH", "").replace("SparkPHP.zip","")
    }


  def create(): Socket = {
    if (false) {
      synchronized {
        if (idleWorkers.size > 0) {
          return idleWorkers.dequeue()
        }
      }
      createThroughDaemon()
    } else {
      createSimpleWorker()
    }
  }


  /**
   * Connect to a worker launched through php//src/daemon.py, which forks php processes itself
   * to avoid the high cost of forking from Java. This currently only works on UNIX-based systems.
   */
  private def createThroughDaemon(): Socket = {

    def createSocket(): Socket = {
      val socket = new Socket(daemonHost, daemonPort)
      val pid = new DataInputStream(socket.getInputStream).readInt()
      if (pid < 0) {
        throw new IllegalStateException("Php daemon failed to launch worker with code " + pid)
      }
      daemonWorkers.put(socket, pid)
      socket
    }

    synchronized {
      // Start the daemon if it hasn't been started
      startDaemon()

      // Attempt to connect, restart and retry once if it fails
      try {
        createSocket()
      } catch {
        case exc: SocketException =>
          logWarning("Failed to open socket to Php daemon:", exc)
          logWarning("Assuming that daemon unexpectedly quit, attempting to restart")
          stopDaemon()
          startDaemon()
          createSocket()
      }
    }
  }


  /**
   * Launch a worker by executing worker.py directly and telling it to connect to us.
   */
  private def createSimpleWorker(): Socket = {
    var serverSocket: ServerSocket = null
    try {
      serverSocket = new ServerSocket(0, 1, InetAddress.getByAddress(Array(127, 0, 0, 1)))

      // Create and start the worker
      val commands = new java.util.ArrayList[String]();
      commands.add(sys.env.getOrElse("SPARKPHP_DRIVER_PHP","/home/map/php7/bin/php"));
      val pb = new ProcessBuilder()
      if(phpSrcPath!=null) {
        commands.add(phpSrcPath+"/src/worker.php");
      }else{
        commands.add("worker.php");
        val tempPhpPath=new java.io.File(sys.env.getOrElse("SPARK_HOME","/home/gt/spark")+"/php/src/");
        pb.directory(tempPhpPath);
      }

      pb.command(commands);

      val workerEnv = pb.environment()
      workerEnv.putAll(envVars.asJava)
    //  workerEnv.put("PHPPATH", phpPath)
   //   workerEnv.put("PHPUNBUFFERED", "YES")
      val worker = pb.start()

      // Redirect worker stdout and stderr
      redirectStreamsToStderr(worker.getInputStream, worker.getErrorStream)

      // Tell the worker our port
      val out = new OutputStreamWriter(worker.getOutputStream)
      out.write(serverSocket.getLocalPort+"\n")
      if(phpSrcPath!=null){
        out.write(phpSrcPath+"\n")
      }else{
        out.write("NULL\n")
      }

      out.flush()

      // Wait for it to connect to our socket
      serverSocket.setSoTimeout(10000)
      try {
        val socket = serverSocket.accept()
        simpleWorkers.put(socket, worker)
        return socket
      } catch {
        case e: Exception =>
          throw new SparkException("Php worker did not connect back in time + " + e.getStackTraceString, e)
      }
    } finally {
      if (serverSocket != null) {
        serverSocket.close()
      }
    }
    null
  }

  private def startDaemon() {
    synchronized {
      // Is it already running?
      if (daemon != null) {
        return
      }

      try {

        val pb = new ProcessBuilder()

        val commands = new java.util.ArrayList[String]();
        commands.add(sys.env.getOrElse("SPARKPHP_DRIVER_PHP","/home/map/php7/bin/php"));

        if(phpSrcPath!=null) {
          commands.add(phpSrcPath+"/src/daemon.php");
        }else{
          commands.add("daemon.php");
          val tempPhpPath=new java.io.File(sys.env.getOrElse("SPARK_HOME","/home/gt/spark")+"/php/src/");
          pb.directory(tempPhpPath);
        }

        pb.command(commands);

        // Create and start the daemon

        val workerEnv = pb.environment()
        workerEnv.putAll(envVars.asJava)
        workerEnv.put("PHPPATH", phpPath)
        // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
        workerEnv.put("PHPUNBUFFERED", "YES")
        daemon = pb.start()

        val in = new DataInputStream(daemon.getInputStream)
        daemonPort = in.readInt()

        // Redirect daemon stdout and stderr
        redirectStreamsToStderr(in, daemon.getErrorStream)

      } catch {
        case e: Exception =>

          // If the daemon exists, wait for it to finish and get its stderr
          val stderr = Option(daemon)
            .flatMap { d => Utils.getStderr(d, PROCESS_WAIT_TIMEOUT_MS) }
            .getOrElse("")

          stopDaemon()

          if (stderr != "") {
            val formattedStderr = stderr.replace("\n", "\n  ")
            val errorMessage = s"""
                                  |Error from php worker:
                                  |  $formattedStderr
                |PHPPATH was:
                |  $phpPath
                |$e"""

            // Append error message from python daemon, but keep original stack trace
            val wrappedException = new SparkException(errorMessage.stripMargin)
            wrappedException.setStackTrace(e.getStackTrace)
            throw wrappedException
          } else {
            throw e
          }
      }

      // Important: don't close daemon's stdin (daemon.getOutputStream) so it can correctly
      // detect our disappearance.
    }
  }

  /**
   * Redirect the given streams to our stderr in separate threads.
   */
  private def redirectStreamsToStderr(stdout: InputStream, stderr: InputStream) {
    try {
      new RedirectThread(stdout, System.err, "stdout reader for " + phpExec).start()
      new RedirectThread(stderr, System.err, "stderr reader for " + phpExec).start()
    } catch {
      case e: Exception =>
        logError("Exception in redirecting streams", e)
    }
  }

  /**
   * Monitor all the idle workers, kill them after timeout.
   */
  private class MonitorThread extends Thread(s"Idle Worker Monitor for $phpExec") {

    setDaemon(true)

    override def run() {
      while (true) {
        synchronized {
          if (lastActivity + IDLE_WORKER_TIMEOUT_MS < System.currentTimeMillis()) {
            cleanupIdleWorkers()
            lastActivity = System.currentTimeMillis()
          }
        }
        Thread.sleep(10000)
      }
    }
  }

  private def cleanupIdleWorkers() {
    while (idleWorkers.length > 0) {
      val worker = idleWorkers.dequeue()
      try {
        // the worker will exit after closing the socket
        worker.close()
      } catch {
        case e: Exception =>
          logWarning("Failed to close worker socket", e)
      }
    }
  }

  private def stopDaemon() {
    synchronized {
      if (useDaemon) {
        cleanupIdleWorkers()

        // Request shutdown of existing daemon by sending SIGTERM
        if (daemon != null) {
          daemon.destroy()
        }

        daemon = null
        daemonPort = 0
      } else {
        simpleWorkers.mapValues(_.destroy())
      }
    }
  }

  def stop() {
    stopDaemon()
  }

  def stopWorker(worker: Socket) {
    synchronized {
      if (useDaemon) {
        if (daemon != null) {
          daemonWorkers.get(worker).foreach { pid =>
            // tell daemon to kill worker by pid
            val output = new DataOutputStream(daemon.getOutputStream)
            output.writeInt(pid)
            output.flush()
            daemon.getOutputStream.flush()
          }
        }
      } else {
        simpleWorkers.get(worker).foreach(_.destroy())
      }
    }
    worker.close()
  }

  def releaseWorker(worker: Socket) {
    if (useDaemon) {
      synchronized {
        lastActivity = System.currentTimeMillis()
        idleWorkers.enqueue(worker)
      }
    } else {
      // Cleanup the worker socket. This will also cause the Python worker to exit.
      try {
        worker.close()
      } catch {
        case e: Exception =>
          logWarning("Failed to close worker socket", e)
      }
    }
  }

}


private object PhpWorkerFactory {
  val PROCESS_WAIT_TIMEOUT_MS = 10000
  val IDLE_WORKER_TIMEOUT_MS = 60000  // kill idle workers after 1 minute
}