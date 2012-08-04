package cloudcmd.common

import java.io.{File, ByteArrayInputStream}
import org.json.JSONArray
import org.json.JSONObject
import util.{CryptoUtil, JsonUtil}

object FileMetaData {
  def fromJson(jsonObject: JSONObject): FileMetaData = {
    fromJson(jsonObject.getString("hash"), jsonObject.getJSONObject("data"))
  }

  def fromJson(hash: String, data: JSONObject): FileMetaData = {
    val meta: FileMetaData = new FileMetaData
    meta._data = data
    meta._hash = hash
    meta
  }

  def toJsonArray(meta: List[FileMetaData]): JSONArray = {
    val result = new JSONArray
    meta.foreach(m => result.put(m.toJson))
    result
  }

  def create(jsonObject: JSONObject): FileMetaData = {
    fromJson(CryptoUtil.computeHashAsString(new ByteArrayInputStream(jsonObject.toString.getBytes("UTF-8"))) + ".meta", jsonObject)
  }

  def create(file: File, blockHashes: List[String], tags: Set[String]): FileMetaData = {
    val fileName = file.getName
    val extIndex = fileName.lastIndexOf(".")
    FileMetaData.create(
      JsonUtil.createJsonObject(
        "path", file.getCanonicalPath,
        "filename", fileName,
        "fileext", if (extIndex >= 0) fileName.substring(extIndex + 1) else null,
        "filesize", file.length.asInstanceOf[AnyRef],
        "filedate", file.lastModified.asInstanceOf[AnyRef],
        "blocks", JsonUtil.toJsonArray(blockHashes),
        "tags", JsonUtil.toJsonArray(tags)))
  }

  def deriveMeta(hash: String, data: JSONObject): FileMetaData = {
    val derivedObj: JSONObject = new JSONObject(data.toString)
    derivedObj.put("parent", hash)
    val derivedHash: String = CryptoUtil.computeHashAsString(new ByteArrayInputStream(derivedObj.toString.getBytes("UTF-8"))) + ".meta"
    val meta: FileMetaData = fromJson(derivedHash, derivedObj)
    meta
  }

  def prepareTags(incomingTags: List[String]): Set[String] = {
    incomingTags.map(_.trim).filter(_.length > 0).flatMap(_.split(",").filter(_.length > 0).toSet).toSet
  }

  def applyTags(tags: Set[String], newTags: Set[String]): Set[String] = {
    val modTags = prepareTags(newTags.toList)
    val subTags = modTags.filter(_.startsWith("-"))
    val rmTags = subTags.map(_.substring(1)).toSet
    val addTags = modTags -- subTags
    (tags -- rmTags) ++ addTags
  }

  def createBlockContext(hash: String, fmd: FileMetaData) : BlockContext = {
    new BlockContext(hash, fmd.getTags)
  }
}

class FileMetaData {
  override def hashCode: Int = {
    _hash.hashCode
  }

  override def equals(other: Any): Boolean = {
    _hash.equals(other)
  }

  def getBlockHashes: JSONArray = {
    _data.getJSONArray("blocks")
  }

  def getTags: Set[String] = {
    JsonUtil.createSet(_data.getJSONArray("tags"))
  }

  def toJson: JSONObject = {
    val obj: JSONObject = new JSONObject
    obj.put("hash", _hash)
    obj.put("data", _data)
    obj
  }

  def getHash: String = {
    _hash
  }

  def getParent: String = {
    if (_data.has("parent")) _data.getString("parent") else null
  }

  def getPath: String = {
    _data.getString("path")
  }

  def getFilename: String = {
    _data.getString("filename")
  }

  def getFileExt: String = {
    if (_data.has("fileext")) _data.getString("fileext") else null
  }

  def getFileDate: Long = {
    _data.getLong("filedate")
  }

  def getFileSize: Long = {
    _data.getLong("filesize")
  }

  def getRawData: JSONObject = {
    _data
  }

  def getDataAsString: String = {
    _data.toString
  }

  def createBlockContext(hash: String) : BlockContext = {
    new BlockContext(hash, this.getTags)
  }

  def createBlockContext : BlockContext = {
    createBlockContext(getHash)
  }

  private var _data: JSONObject = null
  private var _hash: String = null
}