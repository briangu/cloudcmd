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

  @Override
  public AdapterStatus getStatus() throws Exception
  {
    return new AdapterStatus(true, false);
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
    newFile.getParentFile().mkdirs();
    if (newFile.exists())
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
      public void process(File file)
      {
        hashes.add(file.getName());
      }
    });

    return hashes;
  }
}

