package cloudcmd.common

import java.io.{InputStream, File, ByteArrayInputStream}
import org.json.JSONArray
import org.json.JSONObject
import util.{CryptoUtil, JsonUtil}
import java.net.URI

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

  def create(is: InputStream, data: JSONObject): FileMetaData = {
    create(CryptoUtil.computeHashAsString(is) + ".meta", data)
  }

  def create(rawData: JSONObject): FileMetaData = {
    create(CryptoUtil.computeHashAsString(new ByteArrayInputStream(rawData.toString.getBytes("UTF-8"))) + ".meta", rawData)
  }

  def create(file: File, blockHashes: List[String], tags: Set[String], properties: JSONObject = new JSONObject): FileMetaData = {
    FileMetaData.create(
      JsonUtil.createJsonObject(
        "path", file.toURI.toASCIIString,
        "size", file.length.asInstanceOf[AnyRef],
        "date", file.lastModified.asInstanceOf[AnyRef],
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

  override def hashCode: Int = {
    _hash.hashCode
  }

  override def equals(other: Any): Boolean = {
    other.isInstanceOf[FileMetaData] && other.asInstanceOf[FileMetaData].getHash.equals(getHash)
  }

  def getBlockHashes: Seq[String] = {
    val blocks = _data.getJSONArray("blocks")
    (0 until blocks.length).map(blocks.getString)
  }

  def getTags: Set[String] = {
    _tags
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
    if (_data.has("parent")) {
      _data.getString("parent")
    } else {
      null
    }
  }

  def getPath: String = {
    _data.getString("path")
  }

  def getURI: URI = {
    try {
      new URI(getPath)
    } catch {
      case e: Exception => {
        new File(getPath).toURI
      }
    }
  }

  def getType: String = {
    if (_data.has("mimeType")) {
      _data.getString("mimeType")
    } else {
      null
    }
  }

  def getFilename: String = {
    if (_data.has("filename")) {
      _data.getString("filename")
    } else {
      new File(getURI).getName
    }
  }

  def getFileExt: String = {
    if (_data.has("fileext")) {
      _data.getString("fileext")
    } else {
      val fileName = getFilename
      val idx = fileName.lastIndexOf('.')
      if (idx > 0) {
        fileName.substring(idx + 1)
      } else {
        null
      }
    }
  }

  def getDate: Long = {
    if (_data.has("filedate")) {
      _data.getLong("filedate")
    } else {
      _data.getLong("date")
    }
  }

  def getCreatedDate: Long = {
    if (_data.has("createdDate")) {
      _data.getLong("createdDate")
    } else {
      getDate
    }
  }

  def getFileSize: Long = {
    if (_data.has("filesize")) {
      _data.getLong("filesize")
    } else {
      _data.getLong("size")
    }
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

  def hasProperties : Boolean = {
    _data.has("properties")
  }

  def getProperties : JSONObject = {
    if (_data.has("properties")) {
      _data.getJSONObject("properties")
    } else {
      null
    }
  }

  def hasProperty(name: String) : Boolean = {
    hasProperties && getProperties.has(name)
  }

  def getCreatorId: Option[String] = {
    if (hasProperty("creatorId")) {
      Some(getProperties.getString("creatorId"))
    } else if (hasProperty("ownerId")) {
      // use the ownerId as creatorId if creatorId is not present
      Some(getProperties.getString("ownerId"))
    } else {
      None
    }
  }

  def getOwnerId: Option[String] = {
    if (hasProperty("ownerId")) {
      Some(getProperties.getString("ownerId"))
    } else if (hasProperty("creatorId")) {
      // use creatorId as ownerId if ownerId is not present
      Some(getProperties.getString("creatorId"))
    } else {
      None
    }
  }

  def isOwner(ownerId: String): Boolean = {
    getOwnerId match {
      case Some(id) => ownerId == id
      case None => false
    }
  }

  def isCreator(creatorId: String): Boolean = {
    getCreatorId match {
      case Some(id) => creatorId == id
      case None => false
    }
  }

  def isPublic: Boolean = {
    if (hasProperty("isPublic")) {
      getProperties.getBoolean("isPublic")
    } else {
      false
    }
  }

  private var _tags: Set[String] = null
  private var _data: JSONObject = null
  private var _hash: String = null
}