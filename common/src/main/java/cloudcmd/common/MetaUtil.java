package cloudcmd.common;


import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class MetaUtil
{
  public static JSONArray toJsonArray(List<FileMetaData> meta)
      throws JSONException
  {
    JSONArray result = new JSONArray();

    for (FileMetaData metaData : meta)
    {
      result.put(metaData.toJson());
    }

    return result;
  }

  // TODO: support file subblocks
  public static FileMetaData createMeta(File file, List<String> blockHashes, Set<String> tags)
      throws IOException, JSONException
  {

    String fileName = file.getName();

    int extIndex = fileName.lastIndexOf(".");

    FileMetaData meta = FileMetaData.create(
      JsonUtil.createJsonObject(
        "path", file.getCanonicalPath(),
        "filename", fileName,
        "fileext", extIndex >= 0 ? fileName.substring(extIndex + 1) : null,
        "filesize", file.length(),
        "filedate", file.lastModified(),
        "blocks", new JSONArray(blockHashes),
        "tags", new JSONArray(tags)
      ));

    return meta;
  }

  public static FileMetaData loadMeta(JSONObject jsonObject)
      throws JSONException, IOException
  {
    return loadMeta(jsonObject.getString("hash"), jsonObject.getJSONObject("data"));
  }

  public static FileMetaData loadMeta(String hash, JSONObject data)
      throws JSONException
  {
    return FileMetaData.create(hash, data);
  }

  public static FileMetaData deriveMeta(String hash, JSONObject data)
      throws JSONException, IOException
  {
    JSONObject derivedObj = new JSONObject(data.toString()); // TODO: there has to be a better way to clone!
    derivedObj.put("parent", hash);
    String derivedHash = CryptoUtil.computeHashAsString(new ByteArrayInputStream(derivedObj.toString().getBytes("UTF-8"))) + ".meta";
    FileMetaData meta = FileMetaData.create(derivedHash, derivedObj);
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

  public static Set<String> applyTags(Set<String> tags, Set<String> newTags)
  {
    Set<String> addTags = new HashSet<String>();
    Set<String> removeTags = new HashSet<String>();

    for (String tag : newTags)
    {
      if (tag.startsWith("-"))
      {
        removeTags.add(tag.substring(1));
      }
      else
      {
        addTags.add(tag);
      }
    }

    Set<String> appliedTags = new HashSet<String>(tags);

    appliedTags.addAll(addTags);
    appliedTags.removeAll(removeTags);

    return appliedTags;
  }
}
