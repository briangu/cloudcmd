package cloudcmd.common.adapters;

import org.json.JSONObject;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class Adapter
{
  public String Type;
  public Set<String> Tags;
  public JSONObject Config;
  public Integer Tier;

  public Adapter()
  {
  }

  public void init(Integer tier, String type, Set<String> tags, JSONObject config) throws Exception
  {
    Tier = tier;
    Type = type;
    Tags = tags;
    Config = config;
  }

  public boolean acceptsTags(Set<String> tags)
  {
    if (Tags == null || Tags.size() == 0) return true;

    for (String tag : tags)
    {
      if (Tags.contains(tag)) return true;
    }

    return false;
  }

  public abstract void refreshCache() throws Exception;

  public abstract boolean contains(String hash) throws Exception;

  public abstract void shutdown() throws Exception;

  public abstract void store(InputStream data, String hash) throws Exception;

  public abstract InputStream load(String hash) throws Exception;

  public abstract Set<String> describe() throws Exception;

  public abstract void storeTags(ByteArrayInputStream byteArrayInputStream, String hash) throws Exception;

  public abstract Set<String> loadTags(String hash) throws Exception;
}
