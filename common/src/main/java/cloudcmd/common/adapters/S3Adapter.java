package cloudcmd.common.adapters;

import org.json.JSONObject;

import java.io.InputStream;
import java.util.Map;
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
}
