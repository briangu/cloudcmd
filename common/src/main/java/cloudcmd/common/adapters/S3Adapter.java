package cloudcmd.common.adapters;

import java.io.InputStream;
import java.net.URI;
import java.util.Set;

//     "s3://<aws id>:<aws secret>@<bucket>?tier=2&tags=s3"

public class S3Adapter extends Adapter
{
  public S3Adapter()
  {
  }

  @Override
  public void init(Integer tier, String type, Set<String> tags, URI uri) throws Exception
  {
    super.init(tier, type, tags, uri);
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
  public String store(InputStream data) throws Exception
  {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
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
}
