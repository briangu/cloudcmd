package cloudcmd.common.adapters;

import cloudcmd.common.FileHandler;
import cloudcmd.common.FileUtil;
import cloudcmd.common.FileWalker;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

public class FileAdapter extends Adapter
{
  String _rootPath;

  public FileAdapter(String type, Set<String> tags, JSONObject config)
  {
    super(type, tags, config);
  }

  @Override
  public void init() throws Exception
  {
    if (!Config.has("rootPath")) throw new IllegalArgumentException("config missing rootPath");

    _rootPath = Config.getString("rootPath");
  }

  @Override
  public void shutdown()
  {
  }

  private String getPathFromHash(String hash) throws JSONException
  {
    return _rootPath + File.separator + hash.substring(0, 2);
  }

  private String getDataFileFromHash(String hash) throws JSONException
  {
    return getPathFromHash(hash) + File.separator + hash;
  }

  private String getMetaFileFromHash(String hash) throws JSONException
  {
    return getPathFromHash(hash) + ".meta";
  }

  @Override
  public void store(InputStream data, JSONObject meta) throws Exception
  {
    if (!meta.has("hash")) throw new IllegalArgumentException("meta missing hash");
    String hash = meta.getString("hash");

    String path = getPathFromHash(hash);
    new File(path).mkdir();

    FileUtil.writeFile(getDataFileFromHash(hash), data);
    FileUtil.writeFile(getMetaFileFromHash(hash), meta);
  }

  @Override
  public InputStream load(JSONObject meta) throws Exception
  {
    if (!meta.has("hash")) throw new IllegalArgumentException("meta missing hash");
    File file = new File(getDataFileFromHash(meta.getString("hash")));
    if (!file.exists()) throw new DataNotFoundException(meta);
    return new FileInputStream(file);
  }

  @Override
  public Set<JSONObject> describe()
  {
    final Set<JSONObject> allMeta = new HashSet<JSONObject>();

    FileWalker.enumerateFolders(_rootPath, new FileHandler()
    {
      @Override
      public void process(File file)
      {
        if (!file.getName().endsWith(".meta")) return;
        try
        {
          allMeta.add(FileUtil.readJson(file));
        }
        catch (IOException e)
        {
          System.err.println(file.getAbsoluteFile());
          e.printStackTrace();
        }
        catch (JSONException e)
        {
          System.err.println(file.getAbsoluteFile());
          e.printStackTrace();
        }
      }
    });

    return allMeta;
  }
}
