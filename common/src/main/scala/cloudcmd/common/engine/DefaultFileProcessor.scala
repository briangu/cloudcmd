package cloudcmd.common.engine

import java.io.{ByteArrayOutputStream, ByteArrayInputStream, FileInputStream, File}
import cloudcmd.common.{BlockContext, FileMetaData, FileUtil}
import cloudcmd.common.util.{FileTypeUtil, CryptoUtil}
import cloudcmd.common.config.ConfigStorage
import org.json.JSONObject
import javax.imageio.ImageIO
import org.imgscalr.{Scalr, AsyncScalr}

class DefaultFileProcessor(configStorage: ConfigStorage, cloudEngine: CloudEngine, indexStorage: IndexStorage, thumbWidth: Int, thumbHeight: Int) extends FileProcessor {

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

      val mimeType = getMimeType(fmd)
      if (mimeType.startsWith("image")) {
        val ba = createThumbnail(file, thumbWidth, thumbHeight)
        if (ba != null) {
          val bis = new ByteArrayInputStream(ba)
          val hash = CryptoUtil.computeHashAsString(bis)
          bis.reset()
          try {
            cloudEngine.store(new BlockContext(hash), bis)
          } finally {
            bis.close
          }
          fmd.getRawData.put("thumbHash", hash)
          fmd.getRawData.put("thumbSize",  ba.length.toLong)
        }
      }
      fmd.getRawData.put("type", mimeType)

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
          e.printStackTrace
          null
        }
      }
    } else {
      null
    }
  }

  // TODO: use a more canonical set
  private def getMimeType(fmd: FileMetaData) : String = {
    if (fmd.getType != null) return fmd.getType
    Option(fmd.getFileExt) match {
      case Some("jpg") => "image/jpg"
      case Some("gif") => "image/gif"
      case Some("png") => "image/png"
      case _ => "application/octet-stream"
    }
  }
}
