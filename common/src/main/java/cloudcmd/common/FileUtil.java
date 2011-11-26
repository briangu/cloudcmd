package cloudcmd.common;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

public class FileUtil
{
  public static JSONObject readJson(String path) throws IOException, JSONException
  {
    String rawJson = readFile(path);
    JSONObject json = new JSONObject(rawJson);
    return json;
  }

  public static JSONObject readJson(File file) throws IOException, JSONException
  {
    String rawJson = readFile(file);
    JSONObject json = new JSONObject(rawJson);
    return json;
  }

  // http://stackoverflow.com/questions/326390/how-to-create-a-java-string-from-the-contents-of-a-file
  public static String readFile(String path)
      throws IOException
  {
    return readFile(new File(path));
  }

  public static String readFile(File file)
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
