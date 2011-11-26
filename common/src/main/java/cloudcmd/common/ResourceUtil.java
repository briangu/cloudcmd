package cloudcmd.common;

import org.apache.commons.io.IOUtils;
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
    JSONObject obj = new JSONObject(rawJson);
    return obj;
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
    try {
      System.out.println(resourceUrl);
      return FileUtil.readFile(new File(resourceUrl.toURI()));
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return null;
  }
}
