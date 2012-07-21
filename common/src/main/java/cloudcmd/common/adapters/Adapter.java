package cloudcmd.common.adapters;

import org.jboss.netty.buffer.ChannelBuffer;

import java.io.InputStream;
import java.net.URI;
import java.util.Set;

public abstract class Adapter
{
  public String ConfigDir;
  public String Type;
  public Set<String> Tags;
  public URI URI;
  public Integer Tier;
  protected boolean _isOnline;

  public Adapter()
  {
  }

  public boolean IsOnLine()
  {
    return true;
  }

  public boolean IsFull()
  {
    return false;
  }

  // TODO: get tier from the URI

  public void init(String configDir, Integer tier, String type, Set<String> tags, URI uri) throws Exception
  {
    ConfigDir = configDir;
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

  public abstract boolean remove(String hash) throws Exception;

  public abstract boolean verify(String hash) throws Exception;
  
  public abstract void refreshCache() throws Exception;

  public abstract boolean contains(String hash) throws Exception;

  public abstract void store(InputStream data, String hash) throws Exception;

  // store the file and compute the hash at runtime
  // TODO: remove this function as it's only used for internal use
  @Deprecated
  public abstract String store(InputStream data) throws Exception;

  public abstract InputStream load(String hash) throws Exception;

  // TODO: this is a total hack until load is converted to use channels uniformally
  public abstract ChannelBuffer loadChannel(String hash) throws Exception;

  public abstract Set<String> describe() throws Exception;

}
