package cloudcmd.common.adapters;

import org.json.JSONObject;

import java.io.InputStream;
import java.util.Set;

public class S3Adapter extends Adapter
{
  public S3Adapter(String type, Set<String> tags, JSONObject config)
  {
    super(type, tags, config);
  }

  @Override
  public void init() throws Exception
  {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void shutdown()
  {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public void store(InputStream data, JSONObject meta)
  {
    //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public InputStream load(JSONObject meta)
  {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public Set<JSONObject> describe()
  {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
