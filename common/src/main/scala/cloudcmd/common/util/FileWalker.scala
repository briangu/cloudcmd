package cloudcmd.common.util

import java.io.File
import java.util.concurrent.SynchronousQueue

object FileWalker {

  private case class FileTask(file: File, handler: FileWalker.FileHandler)

  private val queue = new SynchronousQueue[Option[FileTask]]

  private val msgPump: Thread = new Thread(new Runnable {
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
      }
    }
  })

  def start() {
    msgPump.start()
  }

  def shutdown() {
    queue.put(None)
    msgPump.join(1000)
  }

  def enumerateFolders(directory: String, handler: FileWalker.FileHandler) {
    val rootDir = new File(directory)
    if (!rootDir.exists) {
      throw new IllegalArgumentException("directory does not exist: " + directory)
    }

    val stack = new collection.mutable.Stack[File]
    stack.push(rootDir)

    while (!stack.isEmpty) {
      val curFile = stack.pop()
      val subFiles = curFile.listFiles
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
  }

  trait FileHandler {
    def skipDir(file: File): Boolean
    def process(file: File)
  }
}