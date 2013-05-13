package cloudcmd.common

import java.io.{File, ByteArrayInputStream}
import org.json.JSONArray
import org.json.JSONObject
import util.{CryptoUtil, JsonUtil}

object FileMetaData {
  def fromJson(serialized: JSONObject): FileMetaData = {
    create(serialized.getString("hash"), serialized.getJSONObject("data"))
  }

  def fromJsonArray(arr: JSONArray): Seq[FileMetaData] = {
    (0 until arr.length()).map(i => FileMetaData.fromJson(arr.getJSONObject(i)))
  }

  def toJsonArray(meta: Iterable[FileMetaData]): JSONArray = {
    val result = new JSONArray
    meta.foreach(m => result.put(m.toJson))
    result
  }

  def toBlockContexts(fmds: Seq[FileMetaData]): Set[BlockContext] = {
    Set() ++ fmds.flatMap(_.createAllBlockContexts)
  }

  def toBlockContextsFromJsonArray(arr: JSONArray, includeBlockHashes: Boolean): Set[BlockContext] = {
    if (includeBlockHashes) {
      Set() ++ (0 until arr.length()).flatMap(i => FileMetaData.fromJson(arr.getJSONObject(i)).createAllBlockContexts)
    } else {
      Set() ++ (0 until arr.length()).flatMap(i => Set(FileMetaData.fromJson(arr.getJSONObject(i)).createBlockContext))
    }
  }

  def create(hash: String, data: JSONObject): FileMetaData = {
    val meta: FileMetaData = new FileMetaData
    meta._data = data
    meta._tags = JsonUtil.createSet(data.getJSONArray("tags"))
    meta._hash = hash
    meta
  }

  def create(rawData: JSONObject): FileMetaData = {
    create(CryptoUtil.computeHashAsString(new ByteArrayInputStream(rawData.toString.getBytes("UTF-8"))) + ".meta", rawData)
  }

  def create(file: File, blockHashes: List[String], tags: Set[String], properties: JSONObject = new JSONObject): FileMetaData = {
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
        "tags", JsonUtil.toJsonArray(tags),
        "properties", if (properties.length > 0) properties else null))
  }

  def deriveMeta(hash: String, data: JSONObject): FileMetaData = {
    val derivedObj: JSONObject = new JSONObject(data.toString)
    derivedObj.put("parent", hash)
    val derivedHash: String = CryptoUtil.computeHashAsString(new ByteArrayInputStream(derivedObj.toString.getBytes("UTF-8"))) + ".meta"
    val meta: FileMetaData = create(derivedHash, derivedObj)
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
    createBlockContext(hash, fmd.getTags)
  }

  def createBlockContext(hash: String, tags: Set[String]) : BlockContext = {
    new BlockContext(hash, tags)
  }
}

class FileMetaData {
  private var _hashCode = 0

  override def hashCode: Int = {
    if (_hashCode == 0) _hashCode = 31 * getHash.hashCode * getRawData.hashCode
    _hashCode
  }

  override def equals(other: Any): Boolean = {
    other.isInstanceOf[FileMetaData] && other.asInstanceOf[FileMetaData].getHash.equals(getHash) && other.asInstanceOf[FileMetaData].getRawData.equals(getRawData)
  }

  def getBlockHashes: Seq[String] = {
    val blocks = _data.getJSONArray("blocks")
    (0 until blocks.length).map(blocks.getString)
  }

  def getThumbHash: String = {
    if (_data.has("thumbHash")) _data.getString("thumbHash") else null
  }

  def getThumbSize: Int = {
    if (_data.has("thumbSize")) _data.getInt("thumbSize") else -1
  }

  def getTags: Set[String] = _tags

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

  def getType: String = if (_data.has("mimeType")) _data.getString("mimeType") else null

  def getFilename: String = {
    _data.getString("filename")
  }

  def getFileExt: String = {
    if (_data.has("fileext")) _data.getString("fileext") else null
  }

  def getFileDate: Long = {
    _data.getLong("filedate")
  }

  def getCreatedDate: Long = {
    if (_data.has("createdDate")) _data.getLong("createdDate") else getFileDate
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

  def createAllBlockContexts: Seq[BlockContext] = {
    List(createBlockContext) ++ createBlockHashBlockContexts
  }

  def createBlockHashBlockContexts: Seq[BlockContext] = {
    getBlockHashes.map(createBlockContext(_))
  }

  def hasProperties : Boolean = _data.has("properties")

  def getProperties : JSONObject = _data.getJSONObject("properties")

  def hasProperty(name: String) : Boolean = {
    hasProperties && getProperties.has(name)
  }

  private var _tags: Set[String] = null
  private var _data: JSONObject = null
  private var _hash: String = null
}