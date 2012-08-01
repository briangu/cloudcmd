package cloudcmd.common.engine

import java.io.File

trait FileProcessor extends EventSource {
  def add(file: File, tags: Set[String])
  def addAll(file: Set[File], tags: Set[String])
}
