package cloudcmd.common.engine

import java.io.{ByteArrayInputStream, FileInputStream, File}
import cloudcmd.common.{FileMetaData, FileUtil}
import cloudcmd.common.util.{FileTypeUtil, CryptoUtil}
import cloudcmd.common.config.ConfigStorage
import org.json.JSONObject

class DefaultFileProcessor(configStorage: ConfigStorage, cloudEngine: CloudEngine, indexStorage: IndexStorage) extends FileProcessor {

  def add(file: File, tags: Set[String], properties: JSONObject) {
    addAll(Set(file), tags)
  }

  def addAll(fileSet: Set[File], tags: Set[String], properties: JSONObject) {
    indexStorage.addAll(fileSet.par.map(processFile(_, tags, properties)).toList)
  }

  def processFile(file: File, tags: Set[String], properties: JSONObject) : FileMetaData = {
    var blockHash: String = null

    val startTime = System.currentTimeMillis
    try {
      var fis = new FileInputStream(file)
      try {
        blockHash = CryptoUtil.computeHashAsString(fis)
      } finally {
        FileUtil.SafeClose(fis)
      }

      val extIdx = file.getName.lastIndexOf(".")
      val fileExt = if (extIdx > -1) file.getName.substring(extIdx + 1) else ""
      val fileType = FileTypeUtil.instance.getTypeFromExtension(fileExt)
      val derivedTags = tags ++ Set(fileExt) ++ (if (fileType.length > 0) Set(fileType) else Set())
      val fmd = FileMetaData.create(file, List(blockHash), derivedTags, properties)

      fis = new FileInputStream(file)
      try {
        cloudEngine.store(fmd.createBlockContext(blockHash), fis)
      } finally {
        FileUtil.SafeClose(fis)
      }

      cloudEngine.store(fmd.createBlockContext(fmd.getHash), new ByteArrayInputStream(fmd.getDataAsString.getBytes("UTF-8")))

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
