package cloudcmd.common;

import org.apache.commons.io.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

public class FileUtil {
  public static JSONObject readJson(String path) throws IOException, JSONException {
    String rawJson = readFile(path);
    JSONObject json = new JSONObject(rawJson);
    return json;
  }

  // http://stackoverflow.com/questions/779519/delete-files-recursively-in-java
  public static void delete(File f) throws IOException {
    if (f.isDirectory()) {
      for (File c : f.listFiles())
        delete(c);
    }
    if (!f.delete())
      throw new FileNotFoundException("Failed to delete file: " + f);
  }

  public static JSONObject readJson(File file) throws IOException, JSONException {
    String rawJson = readFile(file);
    JSONObject json = new JSONObject(rawJson);
    return json;
  }

  // http://stackoverflow.com/questions/326390/how-to-create-a-java-string-from-the-contents-of-a-file
  public static String readFile(String path)
    throws IOException {
    return readFile(new File(path));
  }

  public static String readFile(File file)
    throws IOException {
    FileInputStream fis = new FileInputStream(file);
    try {
      FileChannel fc = fis.getChannel();
      MappedByteBuffer bb = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size());
      return Charset.defaultCharset().decode(bb).toString();
    } finally {
      SafeClose(fis);
    }
  }

  public static void writeFile(String outfile, String object) throws IOException {
    InputStream is = new ByteArrayInputStream(object.getBytes("UTF-8"));
    try {
      writeFile(is, outfile);
    } finally {
      SafeClose(is);
    }
  }

  public static void writeFile(String outfile, JSONObject object) throws IOException {
    ByteArrayInputStream bais;

    try {
      bais = new ByteArrayInputStream(object.toString().getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      e.printStackTrace();
      bais = new ByteArrayInputStream(object.toString().getBytes());
    }

    try {
      writeFile(bais, outfile);
    } finally {
      SafeClose(bais);
    }
  }

  public static void writeFile(InputStream srcData, String dataFile) throws IOException {
    File file = new File(dataFile);
    file.getParentFile().mkdirs();
    FileOutputStream fos = new FileOutputStream(file);
    try {
      IOUtils.copy(srcData, fos);
    } finally {
      fos.close();
    }
  }

  public static String writeFileAndComputeHash(InputStream srcData, File destFile) throws IOException {
    return CryptoUtil.digestToString(CryptoUtil.writeAndComputeHash(srcData, destFile));
  }

  public static String findConfigDir(String curPath, String targetDir) throws IOException {
    if (curPath == null || curPath.isEmpty()) return null;
    File curDir = new File(curPath);
    File dir = new File(curPath + File.separatorChar + targetDir);
    if (dir.exists()) return dir.getCanonicalPath();
    return findConfigDir(curDir.getParent(), targetDir);
  }

  public static String getCurrentWorkingDirectory() throws IOException {
    return new File(".").getCanonicalPath();
  }

  public static void SafeClose(Closeable is) {
    if (is == null) return;

    try {
      is.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
