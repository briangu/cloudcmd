package cloudcmd.common;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Set;

public class FileMetaData
{
  private JSONObject _data;
  private String _hash;

  public JSONArray getBlockHashes()
      throws JSONException
  {
    return _data.getJSONArray("blocks");
  }

  public Set<String> getTags()
      throws JSONException
  {
    return JsonUtil.createSet(_data.getJSONArray("tags"));
  }

  public JSONObject toJson()
      throws JSONException
  {
    JSONObject obj = new JSONObject();
    obj.put("hash", _hash);
    obj.put("data", _data);
    return obj;
  }

  public static FileMetaData create(JSONObject jsonObject)
      throws IOException
  {
    return
      create(
        CryptoUtil.computeHashAsString(new ByteArrayInputStream(jsonObject.toString().getBytes("UTF-8"))) + ".meta",
        jsonObject);
  }

  public static FileMetaData create(String hash, JSONObject data)
  {
    FileMetaData meta = new FileMetaData();
    meta._data = data;
    meta._hash = hash;
    return meta;
  }

  public String getHash()
  {
    return _hash;
  }

  public String getParent()
      throws JSONException
  {
    return _data.has("parent") ? _data.getString("parent") : null;
  }

  public String getPath()
      throws JSONException
  {
    return _data.getString("path");
  }

  public String getFilename()
      throws JSONException
  {
    return _data.getString("filename");
  }

  public String getFileExt()
      throws JSONException
  {
    return _data.has("fileext") ? _data.getString("fileext") : null;
  }

  public Long getFileDate()
      throws JSONException
  {
    return _data.getLong("filedate");
  }

  public Long getFileSize()
      throws JSONException
  {
    return _data.getLong("filesize");
  }

  public String getDataAsString()
  {
    return _data.toString();
  }
}
