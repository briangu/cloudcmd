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

  public AdapterStatus getStatus() throws Exception
  {
    return new AdapterStatus(true, false);
  }

  public abstract void refreshCache() throws Exception;

  public abstract boolean contains(String hash) throws Exception;

  public abstract void store(InputStream data, String hash) throws Exception;

  public abstract InputStream load(String hash) throws Exception;

  public abstract Set<String> describe() throws Exception;
}
