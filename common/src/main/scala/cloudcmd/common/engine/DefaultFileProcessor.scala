package cloudcmd.common.engine

import java.io.{ByteArrayInputStream, FileInputStream, File}
import cloudcmd.common.util.{MetaUtil, CryptoUtil, FileMetaData}
import cloudcmd.common.FileUtil
import cloudcmd.common.config.ConfigStorage

class DefaultFileProcessor(configStorage: ConfigStorage, cloudEngine: CloudEngine, indexStorage: IndexStorage) extends FileProcessor {

  def add(file: File, tags: Set[String]) {
    addAll(Set(file), tags)
  }

  def addAll(fileSet: Set[File], tags: Set[String]) {
    indexStorage.addAll(fileSet.par.map(processFile(_, tags)).toList)
  }

  def processFile(file: File, tags: Set[String]) : FileMetaData = {
    var blockHash: String = null

    val startTime = System.currentTimeMillis
    try {
      var fis = new FileInputStream(file)
      try {
        blockHash = CryptoUtil.computeHashAsString(fis)
      } finally {
        FileUtil.SafeClose(fis)
      }

      val fmd = MetaUtil.createMeta(file, List(blockHash), tags)

      fis = new FileInputStream(file)
      try {
        cloudEngine.store(blockHash, fis, fmd)
      } finally {
        FileUtil.SafeClose(fis)
      }

      cloudEngine.store(fmd.getHash, new ByteArrayInputStream(fmd.getDataAsString.getBytes("UTF-8")), fmd)

      fmd
    }
    finally {
      onMessage("took %6d ms to index %s".format((System.currentTimeMillis - startTime), file.getName))
      if (blockHash == null) {
        onMessage("failed to index file: " + file.getAbsolutePath)
      }
    }
  }
}
