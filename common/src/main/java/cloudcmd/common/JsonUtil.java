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

  public static JSONObject createJson(Object... args) throws JSONException
  {
    if (args.length %2 != 0) throw new IllegalArgumentException("missing last value: args require key/value pairs");

    JSONObject obj = new JSONObject();

    for (int i = 0; i < args.length; i += 2)
    {
      obj.put(args[i].toString(), args[i+1]);
    }

    return obj;
  }
}
