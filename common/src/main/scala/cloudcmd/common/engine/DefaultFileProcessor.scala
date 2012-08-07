package cloudcmd.common.engine

import java.io.{ByteArrayOutputStream, ByteArrayInputStream, FileInputStream, File}
import cloudcmd.common.{BlockContext, FileMetaData, FileUtil}
import cloudcmd.common.util.{JsonUtil, FileTypeUtil, CryptoUtil}
import cloudcmd.common.config.ConfigStorage
import org.json.JSONObject
import javax.imageio.ImageIO
import org.apache.log4j.Logger
import com.thebuzzmedia.imgscalr.{Scalr, AsyncScalr}

class DefaultFileProcessor(configStorage: ConfigStorage, cloudEngine: CloudEngine, indexStorage: IndexStorage, thumbWidth: Int, thumbHeight: Int) extends FileProcessor {

  private val log = Logger.getLogger(classOf[DefaultFileProcessor])

  final val THUMBNAIL_CREATE_THRESHOLD = 128 * 1024 // TODO: come from config

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
      val fileExt = if (extIdx > -1) file.getName.substring(extIdx + 1) else null
      val mimeType = FileTypeUtil.instance.getTypeFromExtension(fileExt)

      val rawFmd =
        JsonUtil.createJsonObject(
          "path", file.getCanonicalPath,
          "filename", file.getName,
          "fileext", fileExt,
          "filesize", file.length.asInstanceOf[AnyRef],
          "filedate", file.lastModified.asInstanceOf[AnyRef],
          "blocks", JsonUtil.toJsonArray(List(blockHash)),
          "tags", JsonUtil.toJsonArray(tags),
          "properties", if (properties.length > 0) properties else null)

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
              cloudEngine.store(new BlockContext(thumbHash), bis)
            } finally {
              bis.close
            }
            rawFmd.put("thumbHash", thumbHash)
            rawFmd.put("thumbSize", ba.length.toLong)
          }
        }
      }
      rawFmd.put("mimeType", mimeType)

      val fmd = FileMetaData.create(rawFmd)

      fis = new FileInputStream(file)
      try {
        cloudEngine.store(fmd.createBlockContext(blockHash), fis)
      } finally {
        FileUtil.SafeClose(fis)
      }

      cloudEngine.store(fmd.createBlockContext, new ByteArrayInputStream(fmd.getDataAsString.getBytes("UTF-8")))

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
