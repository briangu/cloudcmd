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
    val metaSet = new collection.mutable.HashSet[FileMetaData] with collection.mutable.SynchronizedSet[FileMetaData]

    fileSet.par.foreach {
      file =>
        var blockHash: String = null

        val startTime = System.currentTimeMillis
        try {
          var fis = new FileInputStream(file)
          try {
            blockHash = CryptoUtil.computeHashAsString(fis)
          } finally {
            FileUtil.SafeClose(fis)
          }

          fis = new FileInputStream(file)
          try {
            cloudEngine.store(blockHash, fis)
          } finally {
            FileUtil.SafeClose(fis)
          }

          val meta = MetaUtil.createMeta(file, List(blockHash), tags)
          cloudEngine.store(meta.getHash, new ByteArrayInputStream(meta.getDataAsString.getBytes("UTF-8")))
          metaSet.add(meta)
        }
        finally {
          onMessage("took %6d ms to index %s".format((System.currentTimeMillis - startTime), file.getName))
          if (blockHash == null) {
            onMessage("failed to index file: " + file.getAbsolutePath)
          }
        }
    }

    indexStorage.addAll(metaSet.toList)
  }
}
