package cloudcmd.common.util

import java.io.File

object FileWalker {
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
        val toPush = List() ++ subFiles.par.flatMap {
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