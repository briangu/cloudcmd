package cloudcmd.common;

import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.IOUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;

public class ResourceUtil
{
  public static JSONObject loadOps(String opsResource) throws IOException, JSONException
  {
    return loadJson("/ops/" + opsResource);
  }

  public static JSONObject loadJson(String resource) throws IOException, JSONException
  {
    String rawJson = load(resource);
    if (rawJson == null) return null;
    JSONObject obj = new JSONObject(rawJson);
    return obj;
  }

  public static Set<String> loadJsonArrayAsSet(String resource) throws IOException, JSONException
  {
    String rawJson = load(resource);
    if (rawJson == null) return null;
    JSONArray obj = new JSONArray(rawJson);
    Set<String> set = new HashSet<String>();

    for (int i = 0; i < obj.length(); i++)
    {
      set.add(obj.getString(i));
    }

    return set;
  }

  public static String load(String resource) throws IOException
  {
    resource = resource.startsWith("/") ? resource : "/" + resource;

    InputStream is = ResourceUtil.class.getResourceAsStream(resource);
    if (is != null)
    {
      StringWriter writer = new StringWriter();
      IOUtils.copy(is, writer);
      return writer.toString();
    }

    URL resourceUrl = ResourceUtil.class.getResource(resource);
    if (resourceUrl != null)
    {
      try
      {
        return FileUtil.readFile(new File(resourceUrl.toURI()));
      }
      catch (URISyntaxException e)
      {
        e.printStackTrace();
      }
    }

    return null;
  }
}
