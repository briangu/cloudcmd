package cloudcmd.common;

import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
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

  public static void writeFile(String metaFile, JSONObject meta) throws IOException
  {
    ByteArrayInputStream bais;

    try
    {
      bais = new ByteArrayInputStream(meta.toString().getBytes("UTF-8"));
    }
    catch (UnsupportedEncodingException e)
    {
      e.printStackTrace();
      bais = new ByteArrayInputStream(meta.toString().getBytes());
    }

    writeFile(metaFile, bais);
  }

  public static void writeFile(String dataFile, InputStream data) throws IOException
  {
    File file = new File(dataFile);
//    file.mkdir();
    FileOutputStream fos = new FileOutputStream(file);
    try
    {
      IOUtils.copy(data, fos);
    }
    finally
    {
      fos.close();
    }
  }

  public static String findConfigDir(String curPath, String targetDir) throws IOException
  {
    if (curPath == null || curPath.isEmpty()) return null;

    File curDir = new File(curPath);

    File dir = new File(curPath + File.separatorChar + targetDir);

    if (dir.exists()) return dir.getCanonicalPath();

    return findConfigDir(curDir.getParent(), targetDir);
  }

  public static String getCurrentWorkingDirectory() throws IOException
  {
    return new File(".").getCanonicalPath();
  }
}
