package cloudcmd.common;

import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MetaUtil
{
  public static JSONArray createJson(List<FileMetaData> meta)
      throws JSONException
  {
    JSONArray result = new JSONArray();

    for (FileMetaData metaData : meta)
    {
      JSONObject obj = new JSONObject();
      Iterator<String> iter = metaData.Meta.keys();
      while (iter.hasNext())
      {
        String key = iter.next();
        obj.put(key, metaData.Meta.get(key));
      }
      obj.put("hash", metaData.MetaHash);
      result.put(obj);
    }

    return result;
  }

  // TODO: support file subblocks
  public static FileMetaData createMeta(File file, List<String> blockHashes, Set<String> tags)
      throws IOException, JSONException
  {
    FileMetaData meta = new FileMetaData();

    String fileName = file.getName();

    int extIndex = fileName.lastIndexOf(".");

    meta.Tags = tags;
    meta.BlockHashes = new JSONArray(blockHashes);
    meta.Meta = JsonUtil.createJsonObject(
      "path", file.getCanonicalPath(),
      "filename", fileName,
      "fileext", extIndex >= 0 ? fileName.substring(extIndex + 1) : null,
      "filesize", file.length(),
      "filedate", file.lastModified(),
      "blocks", meta.BlockHashes,
      "tags", tags
    );
    meta.MetaHash = CryptoUtil.computeHashAsString(new ByteArrayInputStream(meta.Meta.toString().getBytes("UTF-8"))) + ".meta";

    return meta;
  }

  public static FileMetaData createMeta(JSONObject jsonObject)
      throws JSONException, IOException
  {
    FileMetaData meta = new FileMetaData();

    meta.Tags = JsonUtil.createSet(jsonObject.getJSONArray("tags"));
    meta.BlockHashes = jsonObject.getJSONArray("blocks");
    meta.Meta = JsonUtil.createJsonObject(
      "path", jsonObject.getString("path"),
      "filename", jsonObject.getString("filename"),
      "fileext", jsonObject.has("fileext") ? jsonObject.getString("fileext") : null,
      "filesize", jsonObject.getLong("filesize"),
      "filedate", jsonObject.getLong("filedate"),
      "blocks", meta.BlockHashes,
      "tags", meta.Tags
    );
    meta.MetaHash = CryptoUtil.computeHashAsString(new ByteArrayInputStream(meta.Meta.toString().getBytes("UTF-8"))) + ".meta";

    return meta;
  }

  public static Set<String> prepareTags(List<String> incomingTags)
  {
    Set<String> tags = new HashSet<String>();

    for (String tag : incomingTags)
    {
      tag = tag.trim();
      if (tag.length() == 0) continue;
      String[] parts = tag.split(",");
      for (String part : parts)
      {
        part = part.trim();
        if (part.length() == 0) continue;
        tags.add(part);
      }
    }

    return tags;
  }
}
