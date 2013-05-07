package cloudcmd.srv

import java.io.{ByteArrayOutputStream, ByteArrayInputStream, File}
import cloudcmd.common.{ContentAddressableStorage, RandomAccessFileInputStream, FileMetaData, FileUtil}
import cloudcmd.common.util.{JsonUtil, FileTypeUtil, CryptoUtil}
import org.json.JSONObject
import javax.imageio.ImageIO
import org.apache.log4j.Logger
import com.thebuzzmedia.imgscalr.{Scalr, AsyncScalr}
import java.util.Date
import cloudcmd.common.engine.FileProcessor

class ThumbnailFileProcessor(cas: ContentAddressableStorage, thumbWidth: Int, thumbHeight: Int) extends FileProcessor {

  private val log = Logger.getLogger(classOf[ThumbnailFileProcessor])

  final val THUMBNAIL_CREATE_THRESHOLD = 128 * 1024 // TODO: come from config

  def add(file: File, fileName: String, tags: List[String], properties: JSONObject = null, mimeType: String = null) : FileMetaData = {
    processFile(file, fileName, tags, null, properties)
  }

  def processFile(file: File, fileName: String, tags: List[String], providedMimeType: String = null, properties: JSONObject = null) : FileMetaData = {
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
      val mimeType = if (providedMimeType == null) {
        FileTypeUtil.instance.getTypeFromExtension(fileExt)
      } else {
        providedMimeType
      }

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

/*
      if (mimeType.startsWith("image")) {
        if (file.length < THUMBNAIL_CREATE_THRESHOLD) {
          rawFmd.put("thumbHash", blockHash)
          rawFmd.put("thumbSize", file.length)
        } else {
          val ba = createThumbnail(file, thumbWidth, thumbHeight)
          if (ba != null) {
            val bis = new ByteArrayInputStream(ba)
            val thumbHash = CryptoUtil.computeHashAsString(bis)
            bis.reset()
            try {
              cloudEngine.store(new BlockContext(thumbHash, tags.toSet), bis)
            } finally {
              bis.close
            }
            rawFmd.put("thumbHash", thumbHash)
            rawFmd.put("thumbSize", ba.length.toLong)
          }
        }
      }
*/

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

//      indexStorage.add(fmd)

      fmd
    }
    finally {
      onMessage("took %6d ms to index %s".format((System.currentTimeMillis - startTime), file.getName))
      if (blockHash == null) {
        onMessage("failed to index file: " + file.getAbsolutePath)
      }
    }
  }

  def createThumbnail(srcFile : File, thumbWidth: Int, thumbHeight: Int) : Array[Byte] = {
    if (srcFile.exists()) {
      val os = new ByteArrayOutputStream()
      try {
        val image = ImageIO.read(srcFile)
        if (image != null) {
          val future =
            AsyncScalr.resize(
              image,
              Scalr.Method.BALANCED,
              Scalr.Mode.FIT_TO_WIDTH,
              thumbWidth,
              thumbHeight,
              Scalr.OP_ANTIALIAS)

          val thumbnail = future.get()
          if (thumbnail != null) {
            ImageIO.write(thumbnail, "jpg", os)
          }
          os.toByteArray
        } else {
          null
        }
      }
      catch {
        case e: Exception => {
          log.info("failed to create thumbnail for " + srcFile.getPath)
          null
        }
      }
    } else {
      null
    }
  }
}
