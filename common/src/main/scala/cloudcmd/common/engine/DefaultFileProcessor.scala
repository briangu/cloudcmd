package cloudcmd.common.engine

import java.io.{ByteArrayInputStream, File}
import cloudcmd.common.{ContentAddressableStorage, RandomAccessFileInputStream, FileMetaData, FileUtil}
import cloudcmd.common.util.{JsonUtil, FileTypeUtil, CryptoUtil}
import org.json.JSONObject
import org.apache.log4j.Logger

class DefaultFileProcessor(cas: ContentAddressableStorage) extends FileProcessor {

  private val log = Logger.getLogger(classOf[DefaultFileProcessor])

  def add(file: File, fileName: String, tags: Set[String], properties: JSONObject = null, mimeType: String = null) : FileMetaData = {
    var blockHash: String = null

    val fis = RandomAccessFileInputStream.create(file)
    try {
      fis.mark(0)
      blockHash = CryptoUtil.computeHashAsString(fis)
    } finally {
      fis.reset()
    }

    // Include the extension has this is most likely the tag that adapters will filter by.
    val fileExtensionIdx = file.getName.lastIndexOf('.')
    val extendedTags = if (fileExtensionIdx >= 0) {
      val fileExtension = file.getName.substring(fileExtensionIdx + 1)
      tags ++ Set(fileExtension)
    } else {
      tags
    }

    val rawFmd =
      JsonUtil.createJsonObject(
        "path", file.toURI.toASCIIString,
        "size", file.length.asInstanceOf[AnyRef],
        "date", file.lastModified.asInstanceOf[AnyRef],
        "blocks", JsonUtil.toJsonArray(List(blockHash)),
        "tags", JsonUtil.toJsonArray(extendedTags))

    if (properties != null && properties.length() > 0) {
      rawFmd.put("properties", properties)
    }

    val fmd = FileMetaData.create(rawFmd)

    try {
      cas.store(fmd.createBlockContext(blockHash), fis)
    } catch {
      case e:Exception => {
        log.error("failed to index block for %s".format(file.getAbsoluteFile), e)
        throw e
      }
    } finally {
      FileUtil.SafeClose(fis)
    }

    try {
      cas.store(fmd.createBlockContext, new ByteArrayInputStream(fmd.getDataAsString.getBytes("UTF-8")))
    } catch {
      case e:Exception => {
        log.error("failed to index meta for %s".format(file.getAbsoluteFile), e)
        throw e
      }
    }

    fmd
  }
}
