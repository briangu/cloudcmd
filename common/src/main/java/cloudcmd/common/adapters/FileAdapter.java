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

  public FileAdapter()
  {
  }

  @Override
  public void init(Integer tier, String type, Set<String> tags, JSONObject config) throws Exception
  {
    super.init(tier, type, tags, config);

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

  @Override
  public void store(InputStream data, String hash) throws Exception
  {
    FileUtil.writeFile(getDataFileFromHash(hash), data);
  }

  @Override
  public InputStream load(String hash) throws Exception
  {
    File file = new File(getDataFileFromHash(hash));
    if (!file.exists()) throw new DataNotFoundException(hash);
    return new FileInputStream(file);
  }

  @Override
  public Set<String> describe()
  {
    final Set<String> hashes = new HashSet<String>();

    FileWalker.enumerateFolders(_rootPath, new FileHandler()
    {
      @Override
      public void process(File file)
      {
        hashes.add(file.getName());
      }
    });

    return hashes;
  }
}

