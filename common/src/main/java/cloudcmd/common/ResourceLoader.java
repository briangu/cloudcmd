package cloudcmd.common;

import com.sun.org.apache.bcel.internal.generic.RETURN;
import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

public class ResourceLoader
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

    InputStream is = ResourceLoader.class.getResourceAsStream(resource);
    if (is != null)
    {
      StringWriter writer = new StringWriter();
      IOUtils.copy(is, writer);
      return writer.toString();
    }

    URL resourceUrl = ResourceLoader.class.getResource(resource);
    try {
      System.out.println(resourceUrl);
      return readFile(new File(resourceUrl.toURI()));
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    return null;
  }

  // http://stackoverflow.com/questions/326390/how-to-create-a-java-string-from-the-contents-of-a-file
  private static String readFile(String path)
      throws IOException
  {
    return readFile(new File(path));
  }

  private static String readFile(File file)
      throws IOException
  {
    FileInputStream stream = new FileInputStream(file);
    try
    {
      FileChannel fc = stream.getChannel();
      MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
      /* Instead of using default, pass in a decoder. */
      return Charset.defaultCharset().decode(bb).toString();
    }
    finally
    {
      stream.close();
    }
  }
}
