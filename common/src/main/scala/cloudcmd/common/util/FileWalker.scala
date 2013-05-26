package cloudcmd.common.util

import java.io.File
import java.util.concurrent.{LinkedBlockingQueue, CountDownLatch}

object FileWalker {

  trait FileHandler {
    def skipDir(file: File): Boolean
    def process(file: File)
  }

  def enumerateFolders(directory: String, handler: FileWalker.FileHandler) {
    new FileWalker().enumerateFolders(directory, handler)
  }
}

class FileWalker {

  private case class FileTask(file: File, handler: FileWalker.FileHandler)

  private val SHUTDOWN_DELAY = 5000 // 5 seconds

  private val doneSignal = new CountDownLatch(1)

  private val queue = new LinkedBlockingQueue[Option[FileTask]]

//  import java.util.concurrent.Executors
//  import scala.concurrent._
//
//  implicit val ec = new ExecutionContext {
//    val threadPool = Executors.newFixedThreadPool(threadCount)
//
//    def execute(runnable: Runnable) {
//      threadPool.submit(runnable)
//    }
//
//    def reportFailure(t: Throwable) {}
//  }

  private val fileTaskConsumer: Thread = new Thread(new Runnable {
    def run() {
      try {
        var done = false
        while(!done) {
          queue.take match {
            case Some(task: FileTask) => {
              task.handler.process(task.file)
            }
            case None => {
              done = true
            }
          }
        }
      } catch {
        case e: Exception => ;
      } finally {
        doneSignal.countDown()
      }
    }
  })

  private def start() {
    fileTaskConsumer.start()
  }

  private def shutdown() {
    if (fileTaskConsumer.isAlive) {
      queue.put(None)
      doneSignal.await()
      fileTaskConsumer.join(SHUTDOWN_DELAY)
    }
  }

  def enumerateFolders(directory: String, handler: FileWalker.FileHandler) {

    val rootDir = new File(directory)
    if (!rootDir.exists) {
      throw new IllegalArgumentException("directory does not exist: " + directory)
    }

    start()

    try {

      val stack = new collection.mutable.Stack[File]
      stack.push(rootDir)

      while (!stack.isEmpty) {
        val subFiles = stack.pop().listFiles
        if (subFiles != null) {
          subFiles.foreach {
            file =>
              if (file.isDirectory) {
                if (!handler.skipDir(file)) {
                  stack.push(file)
                }
              } else {
                queue.put(Some(FileTask(file, handler)))
              }
          }
        }
      }
    } finally {
      shutdown()
    }
  }
}
