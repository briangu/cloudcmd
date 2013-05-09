package cloudcmd.common.engine

import java.io.{ByteArrayInputStream, File}
import cloudcmd.common.{ContentAddressableStorage, RandomAccessFileInputStream, FileMetaData, FileUtil}
import cloudcmd.common.util.{JsonUtil, FileTypeUtil, CryptoUtil}
import org.json.JSONObject
import org.apache.log4j.Logger
import java.util.Date

class DefaultFileProcessor(cas: ContentAddressableStorage) extends FileProcessor {

  private val log = Logger.getLogger(classOf[DefaultFileProcessor])

  def add(file: File, fileName: String, tags: Set[String], properties: JSONObject = null, mimeType: String = null) : FileMetaData = {
    var blockHash: String = null

    val startTime = System.currentTimeMillis
    try {
      var fis = RandomAccessFileInputStream.create(file)
      try {
        blockHash = CryptoUtil.computeHashAsString(fis)
      } finally {
        FileUtil.SafeClose(fis)
      }

      val extIdx = file.getName.lastIndexOf(".")
      val fileExt = if (extIdx > -1) file.getName.substring(extIdx + 1) else null
      val mimeType = FileTypeUtil.instance.getTypeFromExtension(fileExt)

      val rawFmd =
        JsonUtil.createJsonObject(
          "path", file.getCanonicalPath,
          "filename", fileName,
          "fileext", fileExt,
          "filesize", file.length.asInstanceOf[AnyRef],
          "filedate", file.lastModified.asInstanceOf[AnyRef],
          "createdDate", new Date().getTime.asInstanceOf[AnyRef],  // TODO: this is not ideal as it forces duplicates
          "blocks", JsonUtil.toJsonArray(List(blockHash)),
          "tags", JsonUtil.toJsonArray(tags))

      if (properties != null && properties.length() > 0) {
        rawFmd.put("properties", properties)
      }

      rawFmd.put("mimeType", mimeType)

      val fmd = FileMetaData.create(rawFmd)

      fis = RandomAccessFileInputStream.create(file)
      try {
        cas.store(fmd.createBlockContext(blockHash), fis)
      } catch {
        case e:Exception => log.error(e)
      } finally {
        FileUtil.SafeClose(fis)
      }

      cas.store(fmd.createBlockContext, new ByteArrayInputStream(fmd.getDataAsString.getBytes("UTF-8")))

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
