package cloudcmd.common.util

import java.io.ByteArrayInputStream
import org.json.JSONArray
import org.json.JSONObject

object FileMetaData {
  def create(jsonObject: JSONObject): FileMetaData = {
    create(CryptoUtil.computeHashAsString(new ByteArrayInputStream(jsonObject.toString.getBytes("UTF-8"))) + ".meta", jsonObject)
  }

  def create(hash: String, data: JSONObject): FileMetaData = {
    val meta: FileMetaData = new FileMetaData
    meta._data = data
    meta._hash = hash
    meta
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

  private var _data: JSONObject = null
  private var _hash: String = null
}