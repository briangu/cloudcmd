package cloudcmd.common;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

public class ResourceLoader
{
  public static File loadOps(String opsResource)
  {
    return load("/ops/"+opsResource);
  }

  public static JSONObject loadJson(String resource) throws IOException, JSONException
  {
    File file = load(resource);

    String rawJson = readFile(file);
    JSONObject obj = new JSONObject(rawJson);

    return obj;
  }

  public static File load(String resource)
  {
    resource = resource.startsWith("/") ? resource : "/" + resource;
    URL resourceUrl = ResourceLoader.class.getResource(resource);
    try {
      return new File(resourceUrl.toURI());
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
