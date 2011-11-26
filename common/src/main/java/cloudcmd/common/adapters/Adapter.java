package cloudcmd.common.adapters;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public abstract class Adapter
{
  public String Type;
  public Set<String> Tags;
  public JSONObject Config;

  public Adapter()
  {
  }

  public void init(String type, Set<String> tags, JSONObject config) throws Exception
  {
    Type = type;
    Tags = tags;
    Config = config;
  }

  public abstract void shutdown() throws Exception;

  public abstract void store(InputStream data, JSONObject meta) throws Exception;
  public abstract InputStream load(JSONObject meta) throws Exception;
  public abstract Set<JSONObject> describe() throws Exception;

}
