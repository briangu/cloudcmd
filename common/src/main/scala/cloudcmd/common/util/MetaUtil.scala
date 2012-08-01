package cloudcmd.common.util

import java.io.ByteArrayInputStream
import java.io.File
import org.json.JSONArray
import org.json.JSONObject

object MetaUtil {
  def toJsonArray(meta: List[FileMetaData]): JSONArray = {
    val result = new JSONArray
    meta.foreach(m => result.put(m.toJson))
    result
  }

  def createMeta(file: File, blockHashes: List[String], tags: Set[String]): FileMetaData = {
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

  def loadMeta(jsonObject: JSONObject): FileMetaData = {
    loadMeta(jsonObject.getString("hash"), jsonObject.getJSONObject("data"))
  }

  def loadMeta(hash: String, data: JSONObject): FileMetaData = {
    FileMetaData.create(hash, data)
  }

  def deriveMeta(hash: String, data: JSONObject): FileMetaData = {
    val derivedObj: JSONObject = new JSONObject(data.toString)
    derivedObj.put("parent", hash)
    val derivedHash: String = CryptoUtil.computeHashAsString(new ByteArrayInputStream(derivedObj.toString.getBytes("UTF-8"))) + ".meta"
    val meta: FileMetaData = FileMetaData.create(derivedHash, derivedObj)
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
}