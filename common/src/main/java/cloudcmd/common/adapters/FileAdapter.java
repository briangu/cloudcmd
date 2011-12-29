package cloudcmd.common.adapters;

import cloudcmd.common.FileHandler;
import cloudcmd.common.FileUtil;
import cloudcmd.common.FileWalker;
import org.json.JSONException;

import java.io.*;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

//     "file:///tmp/storage?tier=1&tags=image,movie,vacation"

public class FileAdapter extends Adapter
{
  String _rootPath;

  public FileAdapter()
  {
  }

  @Override
  public void init(String configDir, Integer tier, String type, Set<String> tags, URI config) throws Exception
  {
    super.init(configDir, tier, type, tags, config);

    _rootPath = URI.getPath();

    initSubDirs(_rootPath);

    File rootPathDir = new File(_rootPath);
    rootPathDir.mkdirs();

    IsOnLine = rootPathDir.exists();
  }

  private static void initSubDirs(String rootPath)
  {
    for (int i = 0; i < 0x100; i++)
    {
      File tmpFile = new File(rootPath + File.separator + String.format("%02x", i));
      tmpFile.mkdirs();
    }
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
    FileUtil.writeFile(is, getDataFileFromHash(hash));
  }

  @Override
  public String store(InputStream is) throws Exception
  {
    File tmpFile = new File(_rootPath + File.separator + UUID.randomUUID().toString() + ".tmp");
    tmpFile.createNewFile();
    String hash = FileUtil.writeFileAndComputeHash(is, tmpFile);
    File newFile = new File(getDataFileFromHash(hash));
    if (newFile.exists() && newFile.length() == tmpFile.length())
    {
      tmpFile.delete();
    }
    else
    {
      Boolean success = tmpFile.renameTo(newFile);
      if (!success)
      {
        tmpFile.delete();
        throw new IOException("failed to move file: " + tmpFile.getAbsolutePath());
      }
    }
    return hash;
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
      public boolean skipDir(File file)
      {
        return false;
      }

      @Override
      public void process(File file)
      {
        hashes.add(file.getName());
      }
    });

    return hashes;
  }
}

