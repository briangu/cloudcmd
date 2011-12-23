package cloudcmd.common.adapters;

import java.io.InputStream;
import java.net.URI;
import java.util.Set;

public abstract class Adapter
{
  public String Type;
  public Set<String> Tags;
  public URI URI;
  public Integer Tier;
  public boolean IsOnLine;
  public boolean IsFull;

  public Adapter()
  {
  }

  public void init(Integer tier, String type, Set<String> tags, URI uri) throws Exception
  {
    Tier = tier;
    Type = type;
    Tags = tags;
    URI = uri;
  }

  public abstract void shutdown() throws Exception;

  public boolean accepts(Set<String> tags)
  {
    if (Tags == null || Tags.size() == 0) return true;

    for (String tag : tags)
    {
      if (Tags.contains(tag)) return true;
    }

    return false;
  }

  public abstract void refreshCache() throws Exception;

  public abstract boolean contains(String hash) throws Exception;

  // TODO: remove this function as it's only used for internal use
  @Deprecated
  public abstract void store(InputStream data, String hash) throws Exception;

  // store the file and compute the hash at runtime
  public abstract String store(InputStream data) throws Exception;

  public abstract InputStream load(String hash) throws Exception;

  public abstract Set<String> describe() throws Exception;
}
