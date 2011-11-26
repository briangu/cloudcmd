package cloudcmd.common;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.Iterator;

public class JsonUtil
{
  public static void mergeLeft(JSONObject dest, JSONObject src) throws JSONException
  {
    Iterator<String> keys = src.keys();

    while(keys.hasNext())
    {
      String key = keys.next();
      dest.put(key, src.get(key));
    }
  }
}
