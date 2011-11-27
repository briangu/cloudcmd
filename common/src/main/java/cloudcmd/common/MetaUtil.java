package cloudcmd.common;

import org.json.JSONArray;
import org.json.JSONException;

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
      meta.MetaHash = CryptoUtil.computeHashAsString(new ByteArrayInputStream(meta.Meta.toString().getBytes())) + ".meta";
      meta.Meta = JsonUtil.createJson(
        "path", file.getCanonicalPath(),
        "filename", fileName,
        "fileext", extIndex >= 0 ? fileName.substring(extIndex) : null,
        "filesize", file.length(),
        "filedate", file.lastModified(),
        "blocks", meta.BlockHashes
      );
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


  private static long getFileSize(File file)
  {
    return file.length();
  }

  public static Set<String> createRowTagSet(String rowTags)
  {
    return new HashSet<String>(Arrays.asList(rowTags.split(" ")));
  }
}
