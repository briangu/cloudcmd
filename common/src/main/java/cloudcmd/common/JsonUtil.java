package cloudcmd.common;

import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.Iterator;

public class JsonUtil {
  public static void mergeLeft(JSONObject dest, JSONObject src) throws JSONException {
    Iterator<String> keys = src.keys();
    while (keys.hasNext()) {
      String key = keys.next();
      dest.put(key, src.get(key));
    }
  }

  public static JSONObject createJsonObject(Object... args) throws JSONException {
    if (args.length % 2 != 0) throw new IllegalArgumentException("missing last value: args require key/value pairs");
    JSONObject obj = new JSONObject();
    for (int i = 0; i < args.length; i += 2) {
      obj.put(args[i].toString(), args[i + 1]);
    }
    return obj;
  }

  public static JSONArray loadJsonArray(InputStream load) throws IOException, JSONException {
    try {
      return new JSONArray(new BufferedReader(new InputStreamReader(new DataInputStream(load))).readLine());
    } finally {
      load.close();
    }
  }

  public static JSONObject loadJson(InputStream is) throws IOException, JSONException {
    try {
      return new JSONObject(new BufferedReader(new InputStreamReader(new DataInputStream(is))).readLine());
    } finally {
      FileUtil.SafeClose(is);
    }
  }

  public static Set<String> createSet(JSONArray array)
    throws JSONException {
    Set<String> set = new HashSet<String>();

    for (int i = 0; i < array.length(); i++) {
      String val = array.getString(i);
      if (val.length() == 0) continue;
      set.add(val);
    }

    return set;
  }

  public static Set<String> createSet(String rowTags, String delimiter) {
    String[] parts = rowTags.split(delimiter);
    Set<String> tags = new HashSet<String>();
    for (String part : parts) {
      if (part.length() == 0) continue;
      tags.add(part);
    }
    return tags;
  }

  public static String prettyToString(JSONObject jsonObject) {
    try {
      return jsonObject.toString(2);
    } catch (JSONException e) {
      return jsonObject.toString();
    }
  }
}
