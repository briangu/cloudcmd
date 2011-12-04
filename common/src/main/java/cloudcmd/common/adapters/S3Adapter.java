package cloudcmd.common.adapters;

import org.json.JSONObject;

import java.io.InputStream;
import java.util.Set;

public class S3Adapter extends Adapter
{
  public S3Adapter()
  {
  }

  @Override
  public void init(Integer tier, String type, Set<String> tags, JSONObject config) throws Exception
  {
    super.init(tier, type, tags, config);
  }

  @Override
  public void refreshCache() throws Exception
  {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean contains(String hash) throws Exception
  {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void shutdown()
  {
  }

  @Override
  public void store(InputStream data, String hash)
  {
  }

  @Override
  public InputStream load(String hash)
  {
    return null;
  }

  @Override
  public Set<String> describe()
  {
    return null;
  }

  @Override
  public void storeTags(String hash, Set<String> tags)
  {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Set<String> loadTags(String hash)
  {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
