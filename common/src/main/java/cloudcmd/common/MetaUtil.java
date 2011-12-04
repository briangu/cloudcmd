package cloudcmd.common;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class MetaUtil
{
  // TODO: support file subblocks
  public static FileMetaData createMeta(File file, Set<String> tags)
  {
    FileMetaData meta = new FileMetaData();

    try
    {
      String fileName = file.getName();

      int extIndex = fileName.lastIndexOf(".");

      meta.Tags = tags;
      meta.BlockHashes = getBlockHashes(file);
      meta.Meta = JsonUtil.createJsonObject(
        "path", file.getCanonicalPath(),
        "filename", fileName,
        "fileext", extIndex >= 0 ? fileName.substring(extIndex + 1) : null,
        "filesize", file.length(),
        "filedate", file.lastModified(),
        "blocks", meta.BlockHashes
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

  static JSONArray getBlockHashes(File file)
  {
    String hash = CryptoUtil.computeHashAsString(file);
    if (hash == null)
    {
      System.err.println("failed to compute hash of " + file.getAbsolutePath());
      return null;
    }

    JSONArray blockHashes = new JSONArray();
    blockHashes.put(hash);

    return blockHashes;
  }

  public static Set<String> createRowTagSet(String rowTags)
  {
    return new HashSet<String>(Arrays.asList(rowTags.split(" ")));
  }

  public static FileMetaData createMeta(JSONObject jsonObject) throws JSONException
  {
    FileMetaData meta = new FileMetaData();

    meta.Tags = createRowTagSet(jsonObject.getString("tags"));
    meta.BlockHashes = jsonObject.getJSONArray("blocks");
    meta.Meta = JsonUtil.createJsonObject(
      "path", jsonObject.getString("path"),
      "filename", jsonObject.getString("filename"),
      "fileext", jsonObject.getString("fileext"),
      "filesize", jsonObject.getLong("filesize"),
      "filedate", jsonObject.getLong("filedate"),
      "blocks", meta.BlockHashes
    );
    meta.MetaHash = jsonObject.getString("hash");

    return meta;
  }
}
