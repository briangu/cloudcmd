package cloudcmd.common;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MetaUtil
{
  // TODO: support file subblocks
  public static FileMetaData createMeta(File file, List<String> blockHashes, Set<String> tags)
  {
    FileMetaData meta = new FileMetaData();

    try
    {
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
      meta.MetaHash = CryptoUtil.computeHashAsString(new ByteArrayInputStream(meta.Meta.toString().getBytes())) + ".meta";
    }
    catch (JSONException e)
    {
      e.printStackTrace();
      meta = null;
    }
    catch (IOException e)
    {
      e.printStackTrace();
      meta = null;
    }

    return meta;
  }

  public static Set<String> createTagSet(String rowTags)
  {
    return new HashSet<String>(Arrays.asList(rowTags.split(" ")));
  }

  public static Set<String> createTagSet(JSONArray tags) throws JSONException
  {
    Set<String> set = new HashSet<String>();

    for (int i = 0; i < set.size(); i++)
    {
      set.add(tags.getString(i));
    }

    return set;
  }

  public static FileMetaData createMeta(JSONObject jsonObject) throws JSONException
  {
    FileMetaData meta = new FileMetaData();

    meta.Tags = createTagSet(jsonObject.getJSONArray("tags"));
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
    meta.MetaHash = jsonObject.getString("hash");

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
