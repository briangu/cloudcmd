package cloudcmd.common;

import java.util.HashSet;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.Iterator;

public class JsonUtil
{
  public static void mergeLeft(JSONObject dest, JSONObject src) throws JSONException
  {
    Iterator<String> keys = src.keys();

    while (keys.hasNext())
    {
      String key = keys.next();
      dest.put(key, src.get(key));
    }
  }

  public static JSONObject createJsonObject(Object... args) throws JSONException
  {
    if (args.length % 2 != 0) throw new IllegalArgumentException("missing last value: args require key/value pairs");

    JSONObject obj = new JSONObject();

    for (int i = 0; i < args.length; i += 2)
    {
      obj.put(args[i].toString(), args[i + 1]);
    }

    return obj;
  }

  public static JSONArray loadJsonArray(InputStream load) throws IOException, JSONException
  {
    try
    {
      DataInputStream bis = new DataInputStream(load);
      BufferedReader br = new BufferedReader(new InputStreamReader(bis));
      String rawJson = br.readLine();
      JSONArray arr = new JSONArray(rawJson);
      return arr;
    }
    finally
    {
      load.close();
    }
  }


  public static JSONObject loadJson(InputStream load) throws IOException, JSONException
  {
    try
    {
      DataInputStream bis = new DataInputStream(load);
      BufferedReader br = new BufferedReader(new InputStreamReader(bis));
      String rawJson = br.readLine();
      JSONObject obj = new JSONObject(rawJson);
      return obj;
    }
    finally
    {
      load.close();
    }
  }

  public static Set<String> createSet(JSONArray array)
      throws JSONException
  {
    Set<String> set = new HashSet<String>();

    for (int i = 0; i < array.length(); i++)
    {
      set.add(array.getString(i));
    }

    return set;
  }
}
