package cloudcmd.common.adapters;

import cloudcmd.common.FileHandler;
import cloudcmd.common.FileUtil;
import cloudcmd.common.FileWalker;
import org.json.JSONException;

import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

//     "file:///tmp/storage?tier=1&tags=image,movie,vacation"

public class FileAdapter extends Adapter
{
  String _rootPath;

  public FileAdapter()
  {
  }

  @Override
  public void init(Integer tier, String type, Set<String> tags, URI config) throws Exception
  {
    super.init(tier, type, tags, config);

    _rootPath = URI.getPath();

    new File(_rootPath).mkdirs();
  }

  @Override
  public void refreshCache() throws Exception
  {
  }

  @Override
  public boolean contains(String hash) throws Exception
  {
    return new File(getDataFileFromHash(hash)).exists();
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
  public void store(InputStream is, String hash) throws Exception
  {
    FileUtil.writeFile(getDataFileFromHash(hash), is);
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

  @Override
  public void storeTags(String hash, Set<String> tags)
  {
  }

  @Override
  public Set<String> loadTags(String hash)
  {
    return new HashSet<String>();
  }
}

