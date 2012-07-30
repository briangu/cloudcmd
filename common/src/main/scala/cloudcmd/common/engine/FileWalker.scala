package cloudcmd.common.engine

import java.io.File
import java.util.Stack

object FileWalker {
  def enumerateFolders(startFolder: String, handler: FileWalker.FileHandler) {
    val rootDir = new File(startFolder)
    if (!rootDir.exists) {
      throw new IllegalArgumentException("file does not exist: " + startFolder)
    }

    val stack = new Stack[File]
    stack.push(rootDir)

    while (!stack.isEmpty) {
      val curFile = stack.pop
      val subFiles = curFile.listFiles
      if (subFiles != null) {
        val toPush = List() ++ subFiles.par.flatMap{
          file =>
            if (file.isDirectory) {
              if (handler.skipDir(file)) {
                Nil
              } else {
                List(file)
              }
            } else {
              handler.process(file)
              Nil
            }
        }
        toPush.foreach(stack.push)
      }
    }
  }

  trait FileHandler {
    def skipDir(file: File): Boolean
    def process(file: File)
  }
}