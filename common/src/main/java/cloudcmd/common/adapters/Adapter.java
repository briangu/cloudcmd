package cloudcmd.common.adapters;

import org.json.JSONObject;

import java.io.File;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;

public abstract class Adapter
{
  public String Type;
  public Set<String> Tags;
  public JSONObject Config;

  public Adapter(String type, Set<String> tags, JSONObject config)
  {
    Type = type;
    Tags = tags;
    Config = config;
  }

  public abstract void init();
  public abstract void shutdown();

  public abstract void store(InputStream data, JSONObject meta);
  public abstract InputStream load(JSONObject meta);
  public abstract Set<JSONObject> describe();

}
